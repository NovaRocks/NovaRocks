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

//! Provider-owned Iceberg metadata-table output schemas: which columns a
//! metadata-table alias exposes, and the Arrow schema its rows are shaped to.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};

use crate::iceberg::spec::TableMetadata;

type MetadataOutputError = novarocks_connector_contract::PureProviderCompileError<
    novarocks_spi::connector::ConnectorError,
>;
type MetadataOutputResult<T> = Result<T, MetadataOutputError>;
type MetadataColumnCreator<'a> =
    dyn FnMut(&str, DataType, bool) -> MetadataOutputResult<MetadataOutputColumn> + 'a;

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum MetadataTableType {
    Files,
    Manifests,
    LogicalIcebergMetadata,
    Snapshots,
    History,
    Refs,
}

#[derive(Clone, Debug)]
pub struct MetadataOutputColumn {
    name: String,
    binding: novarocks_type_contract::FunctionValueType,
    temporal_declaration: Option<crate::iceberg::spec::Type>,
}

impl MetadataOutputColumn {
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn data_type(&self) -> &DataType {
        self.binding.data_type()
    }
    pub fn nullable(&self) -> bool {
        self.binding.nullable()
    }
    pub fn value_type(&self) -> &novarocks_type_contract::FunctionValueType {
        &self.binding
    }
    pub(crate) fn temporal_declaration(&self) -> Option<&crate::iceberg::spec::Type> {
        self.temporal_declaration.as_ref()
    }
}

/// Provider-owned schema for one Iceberg metadata-table alias.
///
/// The table metadata is required for the dynamic partition struct exposed by
/// `$files` and `$entries`; every other alias has a fixed provider schema.
pub fn metadata_table_output_columns(
    metadata_table_type: MetadataTableType,
    metadata: &TableMetadata,
    creation: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
) -> Result<
    Vec<MetadataOutputColumn>,
    novarocks_connector_contract::PureProviderCompileError<
        novarocks_spi::connector::ConnectorError,
    >,
> {
    let mut column = |name: &str, data_type: DataType, nullable: bool| {
        let name = creation.text(name)?;
        let binding = typed::value(&data_type, nullable, creation)?;
        let temporal_declaration =
            crate::typed_read::metadata_temporal_declaration(&data_type, creation)?;
        Ok::<_, MetadataOutputError>(MetadataOutputColumn {
            name,
            binding,
            temporal_declaration,
        })
    };
    let map_int_to = |value: DataType| {
        let entries = DataType::Struct(
            vec![
                Arc::new(Field::new("key", DataType::Int32, false)),
                Arc::new(Field::new("value", value, true)),
            ]
            .into(),
        );
        DataType::Map(Arc::new(Field::new("entries", entries, false)), false)
    };
    let list_of = |value: DataType| DataType::List(Arc::new(Field::new("item", value, true)));
    // The frozen relation contract states these two exactly, and the typed
    // reader produces them; declaring anything narrower here makes analysis
    // disagree with what the scan actually returns.
    let timestamp_tz = || DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    let map_varchar_to_varchar = || {
        let entries = DataType::Struct(
            vec![
                Arc::new(Field::new("key", DataType::Utf8, false)),
                Arc::new(Field::new("value", DataType::Utf8, true)),
            ]
            .into(),
        );
        DataType::Map(Arc::new(Field::new("entries", entries, false)), false)
    };
    let files = |column: &mut MetadataColumnCreator<'_>| -> MetadataOutputResult<
        Vec<MetadataOutputColumn>,
    > {
        let mut columns = vec![
            column("content", DataType::Int32, false)?,
            column("file_path", DataType::Utf8, false)?,
            column("file_format", DataType::Utf8, false)?,
            column("spec_id", DataType::Int32, false)?,
            column("record_count", DataType::Int64, false)?,
            column("file_size_in_bytes", DataType::Int64, false)?,
            column("column_sizes", map_int_to(DataType::Int64), true)?,
            column("value_counts", map_int_to(DataType::Int64), true)?,
            column("null_value_counts", map_int_to(DataType::Int64), true)?,
            column("nan_value_counts", map_int_to(DataType::Int64), true)?,
            column("lower_bounds", map_int_to(DataType::Binary), true)?,
            column("upper_bounds", map_int_to(DataType::Binary), true)?,
            column("split_offsets", list_of(DataType::Int64), true)?,
            column("equality_ids", list_of(DataType::Int32), true)?,
            column("sort_order_id", DataType::Int32, true)?,
            column("key_metadata", DataType::Binary, true)?,
            column("first_row_id", DataType::Int64, true)?,
        ];
        let partition = partition_struct_type(metadata).map_err(|error| {
            novarocks_spi::connector::ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::CorruptData,
                error,
            )
        })?;
        if matches!(&partition, DataType::Struct(fields) if !fields.is_empty()) {
            columns.push(column("partition", partition, true)?);
        }
        Ok(columns)
    };
    match metadata_table_type {
        MetadataTableType::Snapshots => Ok(vec![
            column("committed_at", timestamp_tz(), false)?,
            column("snapshot_id", DataType::Int64, false)?,
            column("parent_id", DataType::Int64, true)?,
            column("operation", DataType::Utf8, true)?,
            column("manifest_list", DataType::Utf8, false)?,
            // A real map, not a rendered string: reading one value out of it
            // is the whole reason the column exists, and a string forces every
            // caller to parse the map back out.
            column("summary", map_varchar_to_varchar(), true)?,
        ]),
        MetadataTableType::History => Ok(vec![
            column("made_current_at", timestamp_tz(), false)?,
            column("snapshot_id", DataType::Int64, false)?,
            column("parent_id", DataType::Int64, true)?,
            column("is_current_ancestor", DataType::Boolean, false)?,
        ]),
        MetadataTableType::Refs => Ok(vec![
            column("name", DataType::Utf8, false)?,
            column("type", DataType::Utf8, false)?,
            column("snapshot_id", DataType::Int64, false)?,
            column("max_reference_age_in_ms", DataType::Int64, true)?,
            column("min_snapshots_to_keep", DataType::Int32, true)?,
            column("max_snapshot_age_in_ms", DataType::Int64, true)?,
        ]),
        MetadataTableType::Files => files(&mut column),
        MetadataTableType::Manifests => Ok(vec![
            column("content", DataType::Int32, false)?,
            column("path", DataType::Utf8, false)?,
            column("length", DataType::Int64, false)?,
            column("partition_spec_id", DataType::Int32, false)?,
            column("added_snapshot_id", DataType::Int64, true)?,
            column("added_data_files_count", DataType::Int32, false)?,
            column("existing_data_files_count", DataType::Int32, false)?,
            column("deleted_data_files_count", DataType::Int32, false)?,
            column("added_rows_count", DataType::Int64, false)?,
            column("existing_rows_count", DataType::Int64, false)?,
            column("deleted_rows_count", DataType::Int64, false)?,
            column(
                "partition_summaries",
                list_of(DataType::Struct(
                    vec![
                        Arc::new(Field::new("contains_null", DataType::Boolean, true)),
                        Arc::new(Field::new("contains_nan", DataType::Boolean, true)),
                        Arc::new(Field::new("lower_bound", DataType::Utf8, true)),
                        Arc::new(Field::new("upper_bound", DataType::Utf8, true)),
                    ]
                    .into(),
                )),
                true,
            )?,
        ]),
        MetadataTableType::LogicalIcebergMetadata => {
            let mut columns = vec![
                column("status", DataType::Int32, false)?,
                column("snapshot_id", DataType::Int64, true)?,
                column("sequence_number", DataType::Int64, true)?,
                column("file_sequence_number", DataType::Int64, true)?,
            ];
            columns.extend(files(&mut column)?);
            Ok(columns)
        }
    }
}

pub(crate) fn partition_source_type(
    metadata: &TableMetadata,
    source_id: i32,
) -> Result<crate::iceberg::spec::Type, String> {
    crate::read_snapshot::partition_source_types(metadata, metadata.current_schema(), source_id)
        .map(|(resolved, _)| crate::iceberg::spec::Type::Primitive(resolved))
}

pub(crate) fn partition_struct_type(metadata: &TableMetadata) -> Result<DataType, String> {
    let specs = metadata
        .partition_specs_iter()
        .map(|spec| spec.as_ref().clone())
        .collect::<Vec<_>>();
    let row = crate::partition_row::UnifiedPartitionRow::of(metadata.current_schema(), &specs)
        .map_err(|error| error.to_string())?;
    Ok(row
        .arrow_type()
        .unwrap_or_else(|| DataType::Struct(Fields::empty())))
}

pub(crate) fn iceberg_type_to_arrow_type(
    ty: &crate::iceberg::spec::Type,
) -> Result<DataType, String> {
    use crate::iceberg::spec::{PrimitiveType, Type};
    match ty {
        Type::Primitive(primitive) => Ok(match primitive {
            PrimitiveType::Boolean => DataType::Boolean,
            PrimitiveType::Int => DataType::Int32,
            PrimitiveType::Long => DataType::Int64,
            PrimitiveType::Float => DataType::Float32,
            PrimitiveType::Double => DataType::Float64,
            PrimitiveType::Decimal { precision, scale } => DataType::Decimal128(
                u8::try_from(*precision)
                    .map_err(|_| format!("iceberg decimal precision out of range: {precision}"))?,
                i8::try_from(*scale)
                    .map_err(|_| format!("iceberg decimal scale out of range: {scale}"))?,
            ),
            PrimitiveType::Date => DataType::Date32,
            PrimitiveType::Time => DataType::Time64(TimeUnit::Microsecond),
            PrimitiveType::Timestamp | PrimitiveType::Timestamptz => {
                DataType::Timestamp(TimeUnit::Microsecond, None)
            }
            PrimitiveType::TimestampNs | PrimitiveType::TimestamptzNs => {
                DataType::Timestamp(TimeUnit::Nanosecond, None)
            }
            PrimitiveType::String | PrimitiveType::Uuid => DataType::Utf8,
            PrimitiveType::Fixed(width) => DataType::FixedSizeBinary(
                i32::try_from(*width)
                    .map_err(|_| format!("iceberg fixed width out of range: {width}"))?,
            ),
            PrimitiveType::Binary => DataType::Binary,
        }),
        Type::Variant(_) => {
            Err("iceberg metadata partition fields do not support Variant".to_owned())
        }
        other => Err(format!(
            "iceberg metadata partition field must be primitive, got {other:?}"
        )),
    }
}

/// The Arrow schema of a metadata table with `output_columns`: their names,
/// nullability and normalized types.
pub fn metadata_output_schema(
    output_columns: &[MetadataOutputColumn],
) -> Result<SchemaRef, String> {
    let fields = output_columns
        .iter()
        .map(|column| {
            Arc::new(Field::new(
                column.name(),
                column.data_type().clone(),
                column.nullable(),
            ))
        })
        .collect::<Vec<_>>();
    Ok(Arc::new(Schema::new(fields)))
}

#[cfg(test)]
fn normalize_metadata_output_type(data_type: &DataType) -> DataType {
    match data_type {
        DataType::List(item) => DataType::List(Arc::new(normalize_metadata_output_field(item))),
        DataType::LargeList(item) => {
            DataType::LargeList(Arc::new(normalize_metadata_output_field(item)))
        }
        DataType::FixedSizeList(item, len) => {
            DataType::FixedSizeList(Arc::new(normalize_metadata_output_field(item)), *len)
        }
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|field| normalize_metadata_output_field(field.as_ref()))
                .collect(),
        ),
        DataType::Map(entries, ordered) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return data_type.clone();
            };
            if fields.len() != 2 {
                return data_type.clone();
            }
            let mut normalized_fields = fields.iter().cloned().collect::<Vec<_>>();
            normalized_fields[0] = Arc::new(
                normalized_fields[0]
                    .as_ref()
                    .clone()
                    .with_data_type(normalize_metadata_output_type(
                        normalized_fields[0].data_type(),
                    ))
                    .with_nullable(false),
            );
            normalized_fields[1] = Arc::new(normalized_fields[1].as_ref().clone().with_data_type(
                normalize_metadata_output_type(normalized_fields[1].data_type()),
            ));
            DataType::Map(
                Arc::new(
                    entries
                        .as_ref()
                        .clone()
                        .with_data_type(DataType::Struct(normalized_fields.into()))
                        .with_nullable(false),
                ),
                *ordered,
            )
        }
        _ => data_type.clone(),
    }
}

#[cfg(test)]
fn normalize_metadata_output_field(field: &Field) -> Field {
    field
        .clone()
        .with_data_type(normalize_metadata_output_type(field.data_type()))
}

mod typed;

/// Complete physical declaration of an existing metadata/routing alias.
/// The alias's advertised carrier is an explicit provider fact; it neither
/// imports Arrow annotations nor declares the table value's nominal domain.
pub(crate) fn metadata_alias_value_type(
    ty: &DataType,
    nullable: bool,
    creation: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
) -> Result<
    novarocks_type_contract::FunctionValueType,
    novarocks_connector_contract::PureProviderCompileError<
        novarocks_spi::connector::ConnectorError,
    >,
> {
    typed::value(ty, nullable, creation)
}

/// Complete paired execution declaration of this exact metadata alias author.
/// The caller chooses explicit FE pure preparation or a real BE creation hold.
pub fn metadata_output_execution_schema(
    columns: &[MetadataOutputColumn],
    creation: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
) -> Result<
    novarocks_connector_contract::ConnectorReadExecutionSchema,
    novarocks_connector_contract::PureProviderCompileError<
        novarocks_spi::connector::ConnectorError,
    >,
> {
    typed::schema(columns, creation)
}

#[cfg(test)]
mod tests {
    use super::{MetadataTableType, normalize_metadata_output_type};
    fn metadata_table_output_columns(
        kind: MetadataTableType,
        metadata: &crate::iceberg::spec::TableMetadata,
    ) -> Result<
        Vec<super::MetadataOutputColumn>,
        novarocks_connector_contract::PureProviderCompileError<
            novarocks_spi::connector::ConnectorError,
        >,
    > {
        let mut request = |_| Ok(());
        let mut work = |_| Ok(());
        let mut creation = novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation::new(
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
            &mut request,
            &mut work,
        );
        super::metadata_table_output_columns(kind, metadata, &mut creation)
    }
    use arrow::datatypes::{DataType, Field};
    use std::sync::Arc;

    fn table_metadata() -> crate::iceberg::spec::TableMetadata {
        use std::collections::HashMap;

        use crate::iceberg::spec::{
            FormatVersion, NestedField, PartitionSpec, PrimitiveType, Schema, SortOrder,
            TableMetadataBuilder, Type,
        };
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .expect("schema");
        TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec().into_unbound(),
            SortOrder::unsorted_order(),
            "file:///metadata-schema-test".to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .expect("metadata builder")
        .build()
        .expect("metadata")
        .metadata
    }

    #[test]
    fn provider_owns_metadata_alias_schemas() {
        let metadata = table_metadata();
        let files = metadata_table_output_columns(MetadataTableType::Files, &metadata)
            .expect("files columns");
        assert_eq!(files.first().map(|column| column.name()), Some("content"));
        assert_eq!(
            files.last().map(|column| column.name()),
            Some("first_row_id")
        );
        assert!(!files.iter().any(|column| column.name() == "partition"));
        let snapshots = metadata_table_output_columns(MetadataTableType::Snapshots, &metadata)
            .expect("snapshots columns");
        assert_eq!(
            snapshots
                .iter()
                .map(|column| column.name())
                .collect::<Vec<_>>(),
            vec![
                "committed_at",
                "snapshot_id",
                "parent_id",
                "operation",
                "manifest_list",
                "summary"
            ]
        );
    }

    #[test]
    fn normalizes_metadata_map_key_nullability() {
        let ty = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Arc::new(Field::new("key", DataType::Int32, true)),
                        Arc::new(Field::new("value", DataType::Int64, true)),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
        let DataType::Map(entries, _) = normalize_metadata_output_type(&ty) else {
            panic!("expected map type");
        };
        let DataType::Struct(fields) = entries.data_type() else {
            panic!("expected map entries struct");
        };
        assert!(!fields[0].is_nullable());
        assert!(fields[1].is_nullable());
    }
}
