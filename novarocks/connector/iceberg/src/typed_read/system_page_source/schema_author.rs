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

//! The system relation's explicit complete value author. All new projection
//! backing is admitted by the creating caller before allocation.

use super::*;
use novarocks_connector_contract::{
    ConnectorReadExecutionSchema, ConnectorSourceSchemaFacts, ConnectorSourceSchemaShape,
};
use novarocks_spi::connector::read_stack::{
    ConnectorReadPublicSchema, ConnectorReadSchemaCreation,
};
use novarocks_type_contract::{ValueField, ValueKind, ValueLayout, ValueTypeRef};
use std::alloc::Layout;
use std::fmt::Write;

type Failure = PureProviderCompileError<ConnectorError>;
pub(crate) type Columns = Vec<(String, FunctionValueType)>;

#[derive(Debug)]
pub(crate) struct StatisticsField {
    pub id: i32,
    pub name: String,
    pub primitive: PrimitiveType,
}

pub(crate) fn statistics_fields(
    schema: &Schema,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<Vec<StatisticsField>, Failure> {
    fn visit(
        schema: &Schema,
        field: &NestedField,
        depth: usize,
        output: &mut Vec<StatisticsField>,
        creation: &mut ConnectorReadSchemaCreation<'_>,
    ) -> Result<(), Failure> {
        creation.work(1)?;
        if depth > novarocks_type_contract::MAX_VALUE_TYPE_DEPTH {
            return Err(extent());
        }
        match field.field_type.as_ref() {
            Type::Primitive(primitive) => {
                let name = schema
                    .name_by_field_id(field.id)
                    .ok_or_else(|| bad("metadata statistics field has no qualified name"))?;
                output.push(StatisticsField {
                    id: field.id,
                    name: creation.text(name)?,
                    primitive: primitive.clone(),
                });
            }
            Type::Struct(row) => {
                for child in row.fields() {
                    visit(schema, child, depth + 1, output, creation)?;
                }
            }
            Type::List(list) => {
                visit(schema, &list.element_field, depth + 1, output, creation)?;
            }
            Type::Map(map) => {
                visit(schema, &map.key_field, depth + 1, output, creation)?;
                visit(schema, &map.value_field, depth + 1, output, creation)?;
            }
            Type::Variant(_) => {}
        }
        Ok(())
    }

    let mut output = creation.reserve(schema.field_id_to_fields().len())?;
    for field in schema.as_struct().fields() {
        visit(schema, field, 1, &mut output, creation)?;
    }
    compact(output, creation)
}
fn bad(message: &'static str) -> Failure {
    invalid(message).into()
}
fn extent() -> Failure {
    ConnectorError::new(
        ConnectorErrorKind::ResourceExhausted,
        "system schema creation exceeds its finite source model",
    )
    .into()
}
fn compact<T>(
    values: Vec<T>,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<Vec<T>, Failure> {
    if values.len() == values.capacity() {
        return Ok(values);
    }
    let mut output = creation.reserve(values.len())?;
    for value in values {
        creation.work(1)?;
        output.push(value);
    }
    Ok(output)
}
fn bind(
    kind: ValueKind,
    layout: DataType,
    nullable: bool,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<FunctionValueType, Failure> {
    creation.work(1)?;
    creation.request(ValueTypeRef::storage_allocation_layout().map_err(|_| extent())?)?;
    let value = ValueTypeRef::try_new(kind, nullable, creation.storage())
        .map_err(|_| bad("invalid system value declaration"))?;
    creation.request(ValueLayout::storage_allocation_layout().map_err(|_| extent())?)?;
    let layout =
        ValueLayout::try_from_normalized_arrow_observed(layout, creation.storage(), |units| {
            creation.work(units).map_err(LayoutError::Caller)
        })
        .map_err(LayoutError::into_failure)?;
    FunctionValueType::try_from_value(value, layout)
        .map_err(|_| bad("system declaration differs from its exact layout"))
}
#[derive(Debug)]
enum LayoutError {
    Caller(Failure),
    Source,
}
impl From<novarocks_type_contract::ExecutionValueTypeError> for LayoutError {
    fn from(_: novarocks_type_contract::ExecutionValueTypeError) -> Self {
        Self::Source
    }
}
impl LayoutError {
    fn into_failure(self) -> Failure {
        match self {
            Self::Caller(cause) => cause,
            Self::Source => bad("invalid system layout declaration"),
        }
    }
}
fn scalar(
    ty: DataType,
    nullable: bool,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<FunctionValueType, Failure> {
    let kind = match &ty {
        DataType::Utf8 => ValueKind::String,
        DataType::Binary => ValueKind::Binary,
        _ => ValueKind::Scalar(ty.clone()),
    };
    bind(kind, ty, nullable, creation)
}
fn primitive(
    ty: &PrimitiveType,
    nullable: bool,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<FunctionValueType, Failure> {
    creation.work(1)?;
    let ty = match ty {
        PrimitiveType::Timestamptz | PrimitiveType::TimestamptzNs => {
            let units = if *ty == PrimitiveType::Timestamptz {
                TimeUnit::Microsecond
            } else {
                TimeUnit::Nanosecond
            };
            let text = creation.text(UTC_TIME_ZONE)?;
            creation.request(
                novarocks_type_contract::owned_resources::layout::arc_layout(
                    Layout::array::<u8>(text.len()).map_err(|_| extent())?,
                )
                .map_err(|_| extent())?,
            )?;
            creation.work(text.len())?;
            DataType::Timestamp(units, Some(Arc::<str>::from(text.as_str())))
        }
        PrimitiveType::Uuid => {
            return bind(
                ValueKind::Nominal(ValueLogicalType::Uuid),
                DataType::FixedSizeBinary(16),
                nullable,
                creation,
            );
        }
        PrimitiveType::Fixed(_) => DataType::Binary,
        _ => iceberg_primitive_to_arrow(ty)?,
    };
    scalar(ty, nullable, creation)
}
pub(crate) fn value_for_primitive(
    ty: &PrimitiveType,
    nullable: bool,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<FunctionValueType, Failure> {
    if crate::temporal::declaration(ty).is_some() {
        let declaration = Type::Primitive(ty.clone());
        Ok(crate::schema_mapping::typed_value::from_declaration(
            &declaration,
            nullable,
            None,
            None,
            creation,
        )?
        .binding)
    } else {
        primitive(ty, nullable, creation)
    }
}

fn clone_value(
    value: &FunctionValueType,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<FunctionValueType, Failure> {
    value.clone_observed(|| creation.work(1))
}
fn add(
    columns: &mut Columns,
    name: &str,
    value: FunctionValueType,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<(), Failure> {
    if columns.len() == columns.capacity() {
        return Err(extent());
    }
    columns.push((creation.text(name)?, value));
    Ok(())
}
fn field(
    name: &str,
    value: &FunctionValueType,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<Arc<Field>, Failure> {
    creation.work(1)?;
    let name = creation.text(name)?;
    creation.arc(Field::new(
        name,
        value.data_type().clone(),
        value.nullable(),
    ))
}
pub(crate) fn row(
    columns: Columns,
    nullable: bool,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<FunctionValueType, Failure> {
    let mut fields = creation.reserve(columns.len())?;
    let mut values = creation.reserve(columns.len())?;
    for (name, value) in columns {
        fields.push(field(&name, &value, creation)?);
        creation.request(ValueField::storage_allocation_layout().map_err(|_| extent())?)?;
        creation.work(1)?;
        values.push(ValueField::new(
            name.into_boxed_str(),
            value.value_type().clone(),
            creation.storage(),
        ));
    }
    let layout = DataType::Struct(creation.arrow_fields(fields)?);
    bind(
        ValueKind::Struct(values.into_boxed_slice()),
        layout,
        nullable,
        creation,
    )
}
fn list(
    item: FunctionValueType,
    nullable: bool,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<FunctionValueType, Failure> {
    let layout = DataType::List(field("item", &item, creation)?);
    bind(
        ValueKind::List(item.value_type().clone()),
        layout,
        nullable,
        creation,
    )
}
fn map(
    key: FunctionValueType,
    value: FunctionValueType,
    nullable: bool,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<FunctionValueType, Failure> {
    let mut fields = creation.reserve(2)?;
    fields.push(field("key", &key, creation)?);
    fields.push(field("value", &value, creation)?);
    let entries = DataType::Struct(creation.arrow_fields(fields)?);
    let name = creation.text("entries")?;
    let layout = DataType::Map(creation.arc(Field::new(name, entries, false))?, false);
    bind(
        ValueKind::Map {
            key: key.value_type().clone(),
            value: value.value_type().clone(),
        },
        layout,
        nullable,
        creation,
    )
}
pub(crate) fn partition_facts(
    metadata: &TableMetadata,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<crate::partition_row::UnifiedPartitionRow, Failure> {
    let specs = metadata_specs(metadata, creation)?;
    partition_facts_for_specs(metadata.current_schema(), &specs, creation)
}

fn partition_facts_for_specs(
    schema: &Schema,
    specs: &[&PartitionSpec],
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<crate::partition_row::UnifiedPartitionRow, Failure> {
    crate::partition_row::UnifiedPartitionRow::of_checked(schema, specs, || creation.work(1))
        .map_err(|cause| match cause {
            crate::partition_row::PartitionRowError::Semantic(cause) => Failure::from(cause),
            crate::partition_row::PartitionRowError::Original(cause) => cause,
        })
}

pub(super) fn partition(
    canonical: &crate::partition_row::UnifiedPartitionRow,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<Option<FunctionValueType>, Failure> {
    if canonical.fields.is_empty() {
        return Ok(None);
    }
    let mut columns: Columns = creation.reserve(canonical.fields.len())?;
    for field in &canonical.fields {
        let value = primitive(&field.primitive, true, creation)?;
        add(&mut columns, &field.name, value, creation)?;
    }
    row(columns, true, creation).map(Some)
}

pub(super) fn bounds(
    schema: &Schema,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<Option<FunctionValueType>, Failure> {
    let fields = statistics_fields(schema, creation)?;
    if fields.is_empty() {
        return Ok(None);
    }
    let mut columns: Columns = creation.reserve(fields.len())?;
    for field in fields {
        let width = field.id.unsigned_abs().checked_ilog10().unwrap_or(0) as usize
            + 1
            + usize::from(field.id.is_negative());
        let mut name = String::from_utf8(creation.reserve::<u8>(width)?)
            .map_err(|_| bad("invalid bounds member name buffer"))?;
        creation.work(width)?;
        write!(&mut name, "{}", field.id).map_err(|_| bad("invalid bounds member name"))?;
        let value = primitive(&field.primitive, true, creation)?;
        columns.push((name, value));
    }
    row(columns, true, creation).map(Some)
}

fn s(
    columns: &mut Columns,
    name: &str,
    ty: DataType,
    nullable: bool,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<(), Failure> {
    let value = scalar(ty, nullable, creation)?;
    add(columns, name, value, creation)
}
fn m(
    columns: &mut Columns,
    name: &str,
    key: DataType,
    value: DataType,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<(), Failure> {
    let key = scalar(key, false, creation)?;
    let value = scalar(value, true, creation)?;
    let value = map(key, value, true, creation)?;
    add(columns, name, value, creation)
}
fn l(
    columns: &mut Columns,
    name: &str,
    ty: DataType,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<(), Failure> {
    let item = scalar(ty, true, creation)?;
    let value = list(item, true, creation)?;
    add(columns, name, value, creation)
}
fn file_fields(
    partition: Option<FunctionValueType>,
    bounds: Option<FunctionValueType>,
    entries: bool,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<Columns, Failure> {
    let mut columns = creation.reserve(if entries { 17 } else { 27 })?;
    for (name, ty) in [
        ("content", DataType::Int32),
        ("file_path", DataType::Utf8),
        ("file_format", DataType::Utf8),
        ("spec_id", DataType::Int32),
    ] {
        s(&mut columns, name, ty, false, creation)?;
    }
    if let Some(partition) = partition {
        add(&mut columns, "partition", partition, creation)?;
    }
    for name in ["record_count", "file_size_in_bytes"] {
        s(&mut columns, name, DataType::Int64, false, creation)?;
    }
    for name in [
        "column_sizes",
        "value_counts",
        "null_value_counts",
        "nan_value_counts",
    ] {
        m(
            &mut columns,
            name,
            DataType::Int32,
            DataType::Int64,
            creation,
        )?;
    }
    if entries {
        for name in ["lower_bounds", "upper_bounds"] {
            m(
                &mut columns,
                name,
                DataType::Int32,
                DataType::Utf8,
                creation,
            )?;
        }
    } else if let Some(bounds) = bounds {
        let lower = clone_value(&bounds, creation)?;
        add(&mut columns, "lower_bounds", lower, creation)?;
        add(&mut columns, "upper_bounds", bounds, creation)?;
    }
    s(
        &mut columns,
        "key_metadata",
        DataType::Binary,
        true,
        creation,
    )?;
    l(&mut columns, "split_offsets", DataType::Int64, creation)?;
    l(&mut columns, "equality_ids", DataType::Int32, creation)?;
    s(
        &mut columns,
        "sort_order_id",
        DataType::Int32,
        true,
        creation,
    )?;
    Ok(columns)
}

fn readable_metrics(
    columns: &mut Columns,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<(), Failure> {
    let value = bind(
        ValueKind::Nominal(ValueLogicalType::Json),
        DataType::LargeBinary,
        true,
        creation,
    )?;
    add(columns, "readable_metrics", value, creation)
}

pub(super) fn files(
    partition: Option<FunctionValueType>,
    bounds: Option<FunctionValueType>,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<Columns, Failure> {
    let mut columns = file_fields(partition, bounds, false, creation)?;
    readable_metrics(&mut columns, creation)?;
    for (name, ty, nullable) in [
        ("added_snapshot_id", DataType::Int64, true),
        ("file_sequence_number", DataType::Int64, true),
        ("data_sequence_number", DataType::Int64, true),
        ("referenced_data_file", DataType::Utf8, true),
        ("pos", DataType::Int64, true),
        ("manifest_location", DataType::Utf8, false),
        ("first_row_id", DataType::Int64, true),
        ("content_offset", DataType::Int64, true),
        ("content_size_in_bytes", DataType::Int64, true),
    ] {
        s(&mut columns, name, ty, nullable, creation)?;
    }
    compact(columns, creation)
}

pub(super) fn entries(
    partition: Option<FunctionValueType>,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<Columns, Failure> {
    let mut columns = creation.reserve(6)?;
    s(&mut columns, "status", DataType::Int32, false, creation)?;
    for name in ["snapshot_id", "sequence_number", "file_sequence_number"] {
        s(&mut columns, name, DataType::Int64, true, creation)?;
    }
    let children = file_fields(partition, None, true, creation)?;
    let data_file = row(children, false, creation)?;
    add(&mut columns, "data_file", data_file, creation)?;
    readable_metrics(&mut columns, creation)?;
    Ok(columns)
}
fn snapshots(creation: &mut ConnectorReadSchemaCreation<'_>) -> Result<Columns, Failure> {
    let mut columns = creation.reserve(6)?;
    let time = primitive(&PrimitiveType::Timestamptz, false, creation)?;
    add(&mut columns, "committed_at", time, creation)?;
    s(
        &mut columns,
        "snapshot_id",
        DataType::Int64,
        false,
        creation,
    )?;
    s(&mut columns, "parent_id", DataType::Int64, true, creation)?;
    s(&mut columns, "operation", DataType::Utf8, true, creation)?;
    s(
        &mut columns,
        "manifest_list",
        DataType::Utf8,
        false,
        creation,
    )?;
    m(
        &mut columns,
        "summary",
        DataType::Utf8,
        DataType::Utf8,
        creation,
    )?;
    Ok(columns)
}
fn history(creation: &mut ConnectorReadSchemaCreation<'_>) -> Result<Columns, Failure> {
    let mut columns = creation.reserve(4)?;
    let time = primitive(&PrimitiveType::Timestamptz, false, creation)?;
    add(&mut columns, "made_current_at", time, creation)?;
    s(
        &mut columns,
        "snapshot_id",
        DataType::Int64,
        false,
        creation,
    )?;
    s(&mut columns, "parent_id", DataType::Int64, true, creation)?;
    s(
        &mut columns,
        "is_current_ancestor",
        DataType::Boolean,
        false,
        creation,
    )?;
    Ok(columns)
}
fn refs(creation: &mut ConnectorReadSchemaCreation<'_>) -> Result<Columns, Failure> {
    let mut columns = creation.reserve(6)?;
    s(&mut columns, "name", DataType::Utf8, false, creation)?;
    s(&mut columns, "type", DataType::Utf8, false, creation)?;
    s(
        &mut columns,
        "snapshot_id",
        DataType::Int64,
        false,
        creation,
    )?;
    s(
        &mut columns,
        "max_reference_age_in_ms",
        DataType::Int64,
        true,
        creation,
    )?;
    s(
        &mut columns,
        "min_snapshots_to_keep",
        DataType::Int32,
        true,
        creation,
    )?;
    s(
        &mut columns,
        "max_snapshot_age_in_ms",
        DataType::Int64,
        true,
        creation,
    )?;
    Ok(columns)
}
fn manifests(creation: &mut ConnectorReadSchemaCreation<'_>) -> Result<Columns, Failure> {
    let mut columns = creation.reserve(12)?;
    for (name, ty) in [
        ("content", DataType::Int32),
        ("path", DataType::Utf8),
        ("length", DataType::Int64),
        ("partition_spec_id", DataType::Int32),
    ] {
        s(&mut columns, name, ty, false, creation)?;
    }
    s(
        &mut columns,
        "added_snapshot_id",
        DataType::Int64,
        true,
        creation,
    )?;
    for (name, ty) in [
        ("added_data_files_count", DataType::Int32),
        ("added_rows_count", DataType::Int64),
        ("existing_data_files_count", DataType::Int32),
        ("existing_rows_count", DataType::Int64),
        ("deleted_data_files_count", DataType::Int32),
        ("deleted_rows_count", DataType::Int64),
    ] {
        s(&mut columns, name, ty, true, creation)?;
    }
    let mut summary = creation.reserve(4)?;
    s(
        &mut summary,
        "contains_null",
        DataType::Boolean,
        true,
        creation,
    )?;
    s(
        &mut summary,
        "contains_nan",
        DataType::Boolean,
        true,
        creation,
    )?;
    s(&mut summary, "lower_bound", DataType::Utf8, true, creation)?;
    s(&mut summary, "upper_bound", DataType::Utf8, true, creation)?;
    let summary = row(summary, true, creation)?;
    let summary = list(summary, true, creation)?;
    add(&mut columns, "partition_summaries", summary, creation)?;
    Ok(columns)
}
pub(super) fn columns(
    kind: IcebergSystemTableType,
    schema: &Schema,
    specs: &[&PartitionSpec],
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<Columns, Failure> {
    if matches!(
        kind,
        IcebergSystemTableType::Files | IcebergSystemTableType::Entries
    ) {
        let partition = partition_facts_for_specs(schema, specs, creation)?;
        return columns_from_facts(kind, schema, &partition, creation);
    }
    fixed_columns(kind, creation)
}

pub(crate) fn columns_from_facts(
    kind: IcebergSystemTableType,
    schema: &Schema,
    facts: &crate::partition_row::UnifiedPartitionRow,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<Columns, Failure> {
    creation.work(1)?;
    match kind {
        IcebergSystemTableType::Files => {
            let partition = partition(facts, creation)?;
            let bounds = bounds(schema, creation)?;
            files(partition, bounds, creation)
        }
        IcebergSystemTableType::Entries => {
            let partition = partition(facts, creation)?;
            entries(partition, creation)
        }
        _ => fixed_columns(kind, creation),
    }
}

pub(super) fn fixed_columns(
    kind: IcebergSystemTableType,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<Columns, Failure> {
    creation.work(1)?;
    match kind {
        IcebergSystemTableType::Files => files(None, None, creation),
        IcebergSystemTableType::Entries => entries(None, creation),
        IcebergSystemTableType::Snapshots => snapshots(creation),
        IcebergSystemTableType::History => history(creation),
        IcebergSystemTableType::Refs => refs(creation),
        IcebergSystemTableType::Manifests => manifests(creation),
    }
}

pub(crate) fn output_columns(
    kind: IcebergSystemTableType,
    metadata: &TableMetadata,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<Columns, Failure> {
    if matches!(
        kind,
        IcebergSystemTableType::Files | IcebergSystemTableType::Entries
    ) {
        let facts = partition_facts(metadata, creation)?;
        columns_from_facts(kind, metadata.current_schema(), &facts, creation)
    } else {
        fixed_columns(kind, creation)
    }
}

fn metadata_specs<'a>(
    metadata: &'a TableMetadata,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<Vec<&'a PartitionSpec>, Failure> {
    let mut specs = creation.reserve(metadata.partition_specs_iter().len())?;
    for spec in metadata.partition_specs_iter() {
        creation.work(1)?;
        specs.push(spec.as_ref());
    }
    Ok(specs)
}

/// Publish only selected columns, borrowing the real frozen system relation's
/// formal metadata and the caller's exact creation/control context.
#[cfg(test)]
pub(crate) fn publish_read_schema(
    kind: IcebergSystemTableType,
    requested: &[&IcebergColumnHandle],
    schema: &Schema,
    specs: &[&PartitionSpec],
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<ConnectorReadPublicSchema, Failure> {
    publish_read_schema_inner(requested, creation, |creation| {
        columns(kind, schema, specs, creation)
    })
}

/// The production author borrows the complete metadata generation.
#[cfg(test)]
pub(crate) fn publish_read_schema_with_metadata(
    kind: IcebergSystemTableType,
    requested: &[&IcebergColumnHandle],
    metadata: &TableMetadata,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<ConnectorReadPublicSchema, Failure> {
    publish_read_schema_inner(requested, creation, |creation| {
        output_columns(kind, metadata, creation)
    })
}

pub(crate) fn publish_read_schema_from_facts(
    kind: IcebergSystemTableType,
    requested: &[&IcebergColumnHandle],
    schema: &Schema,
    partitions: &crate::partition_row::UnifiedPartitionRow,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<ConnectorReadPublicSchema, Failure> {
    publish_read_schema_inner(requested, creation, |creation| {
        columns_from_facts(kind, schema, partitions, creation)
    })
}

fn publish_read_schema_inner(
    requested: &[&IcebergColumnHandle],
    creation: &mut ConnectorReadSchemaCreation<'_>,
    author: impl FnOnce(&mut ConnectorReadSchemaCreation<'_>) -> Result<Columns, Failure>,
) -> Result<ConnectorReadPublicSchema, Failure> {
    creation.work(1)?;
    let authored = if requested.is_empty() {
        Vec::new()
    } else {
        author(creation)?
    };
    let mut selected = creation.reserve(requested.len())?;
    let mut fields = creation.reserve(requested.len())?;
    let mut execution_fields = None;
    for column in requested {
        let mut found = None;
        for (name, value) in &authored {
            creation.work(name.len() + 1)?;
            if name == column.base_column_identity().name() {
                found = Some((name, value));
                break;
            }
        }
        let (name, value) = found.ok_or_else(|| bad("system relation has no requested column"))?;
        if !column.is_base_column() || value.nullable() != column.nullable() {
            return Err(bad(
                "system source column differs from its complete declaration",
            ));
        }
        let field = field(name, value, creation)?;
        creation.work(column.type_json().len())?;
        let declaration = super::super::column_handle::parse_type(column.type_json(), "type_json")?;
        creation.work(1)?;
        private_value(
            &declaration,
            value.value_type(),
            value.data_type(),
            creation,
        )?;
        creation.work(column.base_type_json().len())?;
        let base =
            super::super::column_handle::parse_type(column.base_type_json(), "base_type_json")?;
        creation.work(1)?;
        if base != declaration {
            return Err(bad(
                "system base column declaration differs from its projected declaration",
            ));
        }
        private_identity(
            column.base_column_identity(),
            &declaration,
            value.data_type(),
            creation,
        )?;
        let execution = if crate::temporal::contains_temporal(&declaration) {
            crate::schema_mapping::typed_value::from_declaration(
                &declaration,
                value.nullable(),
                None,
                None,
                creation,
            )?
            .binding
        } else {
            clone_value(value, creation)?
        };
        let changed_layout = field.data_type() != execution.data_type();
        if changed_layout && execution_fields.is_none() {
            let mut target = creation.reserve(requested.len())?;
            for prior in &fields {
                creation.work(1)?;
                target.push(Arc::clone(prior));
            }
            execution_fields = Some(target);
        }
        if let Some(target) = &mut execution_fields {
            let execution_field = if changed_layout {
                self::field(name, &execution, creation)?
            } else {
                field.clone()
            };
            target.push(execution_field);
        }
        selected.push(execution);
        fields.push(field);
    }
    let fields = creation.arrow_fields(fields)?;
    let schema = creation.arc(ArrowSchema::new(fields))?;
    let shape = ConnectorSourceSchemaShape::try_new_observed(
        schema.clone(),
        creation.storage(),
        |units| creation.work(units),
    )?;
    let storage = creation.storage();
    let (request, work) = creation.observers();
    // System relation IDs are private connector identity vocabulary; no file
    // assigns these occurrences external field IDs or initial defaults.
    let source = ConnectorSourceSchemaFacts::try_new_observed(
        shape,
        HashMap::new(),
        Box::default(),
        storage.clone(),
        |layout| request(layout).map_err(Failure::Control),
        |units| work(units).map_err(Failure::Control),
    )?;
    let execution_schema = if let Some(execution_fields) = execution_fields {
        let execution_fields = creation.arrow_fields(execution_fields)?;
        creation.arc(ArrowSchema::new(execution_fields))?
    } else {
        schema
    };
    let (request, work) = creation.observers();
    let execution = ConnectorReadExecutionSchema::try_new_observed(
        execution_schema,
        selected.into_boxed_slice(),
        storage.clone(),
        |layout| request(layout).map_err(Failure::Control),
        |units| work(units).map_err(Failure::Control),
    )?;
    let (request, work) = creation.observers();
    ConnectorReadPublicSchema::try_new_typed_observed(
        &execution,
        &source,
        storage,
        |layout| request(layout).map_err(Failure::Control),
        |units| work(units).map_err(Failure::Control),
    )
}
fn private_value(
    source: &Type,
    value: &ValueTypeRef,
    layout: &DataType,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<(), Failure> {
    creation.work(1)?;
    match source {
        Type::Variant(_) => {
            return Err(bad(
                "system private variant is neither a partition value nor a bound",
            ));
        }
        Type::Primitive(primitive) => {
            let layout_matches = match (primitive, layout) {
                (
                    PrimitiveType::Timestamptz,
                    DataType::Timestamp(TimeUnit::Microsecond, Some(tz)),
                )
                | (
                    PrimitiveType::TimestamptzNs,
                    DataType::Timestamp(TimeUnit::Nanosecond, Some(tz)),
                ) => tz.as_ref() == UTC_TIME_ZONE,
                (PrimitiveType::Binary, DataType::LargeBinary) => {
                    value.domain() == Some(ValueLogicalType::Json)
                }
                _ => iceberg_primitive_to_arrow(primitive)? == *layout,
            };
            let semantic_matches = match value.kind() {
                ValueKind::String => *primitive == PrimitiveType::String,
                ValueKind::Binary => *primitive == PrimitiveType::Binary,
                ValueKind::Scalar(scalar) => scalar == layout,
                ValueKind::Nominal(ValueLogicalType::Uuid) => *primitive == PrimitiveType::Uuid,
                ValueKind::Nominal(ValueLogicalType::Json) => *primitive == PrimitiveType::Binary,
                _ => false,
            };
            if !layout_matches || !semantic_matches {
                return Err(bad(
                    "system private primitive differs from complete source declaration",
                ));
            }
        }
        Type::Struct(source) => {
            let (ValueKind::Struct(values), DataType::Struct(fields)) = (value.kind(), layout)
            else {
                return Err(bad("system private struct differs"));
            };
            if source.fields().len() != values.len() || fields.len() != values.len() {
                return Err(bad("system private struct arity differs"));
            }
            for ((source, value), field) in source.fields().iter().zip(values).zip(fields) {
                creation.work(source.name.len() + 1)?;
                if source.name != value.name()
                    || source.name != *field.name()
                    || value.value_type().nullable() == source.required
                    || field.is_nullable() == source.required
                {
                    return Err(bad("system private struct child differs"));
                }
                private_value(
                    &source.field_type,
                    value.value_type(),
                    field.data_type(),
                    creation,
                )?;
            }
        }
        Type::List(source) => {
            let (ValueKind::List(value), DataType::List(field)) = (value.kind(), layout) else {
                return Err(bad("system private list differs"));
            };
            if value.nullable() == source.element_field.required
                || field.is_nullable() == source.element_field.required
            {
                return Err(bad("system list NULL contract differs"));
            }
            private_value(
                &source.element_field.field_type,
                value,
                field.data_type(),
                creation,
            )?;
        }
        Type::Map(source) => {
            let (ValueKind::Map { key, value }, DataType::Map(entries, false)) =
                (value.kind(), layout)
            else {
                return Err(bad("system private map differs"));
            };
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(bad("system map entries differs"));
            };
            if fields.len() != 2
                || key.nullable() == source.key_field.required
                || value.nullable() == source.value_field.required
            {
                return Err(bad("system map NULL contract differs"));
            }
            private_value(
                &source.key_field.field_type,
                key,
                fields[0].data_type(),
                creation,
            )?;
            private_value(
                &source.value_field.field_type,
                value,
                fields[1].data_type(),
                creation,
            )?;
        }
    }
    Ok(())
}
fn private_identity(
    identity: &super::super::ColumnIdentity,
    source: &Type,
    layout: &DataType,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<(), Failure> {
    creation.work(1)?;
    let category = match source {
        Type::Primitive(_) | Type::Variant(_) => super::super::ColumnIdentityCategory::Primitive,
        Type::Struct(_) => super::super::ColumnIdentityCategory::Struct,
        Type::List(_) => super::super::ColumnIdentityCategory::Array,
        Type::Map(_) => super::super::ColumnIdentityCategory::Map,
    };
    if identity.category() != category {
        return Err(bad("system identity category differs from declaration"));
    }
    let children = identity.children();
    match (source, layout) {
        (Type::Primitive(_) | Type::Variant(_), _) => {
            if !children.is_empty() {
                return Err(bad("system scalar identity has children"));
            }
        }
        (Type::List(list), DataType::List(field)) => {
            if children.len() != 1 {
                return Err(bad("system list identity arity differs"));
            }
            private_identity_field(
                &children[0],
                &list.element_field,
                &list.element_field.name,
                field.data_type(),
                creation,
            )?;
        }
        (Type::Map(map), DataType::Map(entries, _)) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(bad("system map identity has no entries"));
            };
            if children.len() != 2 || fields.len() != 2 {
                return Err(bad("system map identity arity differs"));
            }
            private_identity_field(
                &children[0],
                &map.key_field,
                fields[0].name(),
                fields[0].data_type(),
                creation,
            )?;
            private_identity_field(
                &children[1],
                &map.value_field,
                fields[1].name(),
                fields[1].data_type(),
                creation,
            )?;
        }
        (Type::Struct(fields), DataType::Struct(layouts)) => {
            if fields.fields().len() != children.len() || layouts.len() != children.len() {
                return Err(bad("system struct identity arity differs"));
            }
            for ((identity, field), layout) in children.iter().zip(fields.fields()).zip(layouts) {
                private_identity_field(identity, field, &field.name, layout.data_type(), creation)?;
            }
        }
        _ => return Err(bad("system identity layout differs from its formal shape")),
    }
    Ok(())
}
fn private_identity_field(
    identity: &super::super::ColumnIdentity,
    field: &NestedField,
    source_name: &str,
    layout: &DataType,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<(), Failure> {
    // Identity names belong to the formal Iceberg declaration. The independent
    // Arrow author supplies physical carriers (for example List `item`), whose
    // shape and NULL contract are checked separately above.
    creation.work(source_name.len() + identity.name().len() + 1)?;
    if identity.field_id() != field.id || identity.name() != source_name {
        return Err(bad("system identity differs from formal source child"));
    }
    private_identity(identity, &field.field_type, layout, creation)
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_type_contract::{ValueTypeStorageHold, ValueTypeStorageLifetime};
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Hold(Arc<AtomicUsize>);
    impl ValueTypeStorageHold for Hold {}
    impl Drop for Hold {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    fn schema() -> Schema {
        Schema::builder()
            .with_fields([Arc::new(NestedField::optional(
                17,
                "a",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .unwrap()
    }
    #[test]
    fn genuine_system_authors_publish_complete_physical_children_nulls_and_exact_utc() {
        let schema = schema();
        for kind in [
            IcebergSystemTableType::Files,
            IcebergSystemTableType::Entries,
            IcebergSystemTableType::Snapshots,
            IcebergSystemTableType::History,
            IcebergSystemTableType::Refs,
            IcebergSystemTableType::Manifests,
        ] {
            let original = system_relation_schema(kind, &schema, &[]).unwrap();
            let Type::Struct(row) = metadata_relation_row(&original).unwrap() else {
                panic!("system declaration is a row");
            };
            let columns: Vec<_> = row
                .fields()
                .iter()
                .map(|field| IcebergColumnHandle::base_column(field).unwrap())
                .collect();
            let borrowed: Vec<_> = columns.iter().collect();
            let mut requests = Vec::new();
            let mut observed = 0usize;
            let mut request = |layout: Layout| {
                requests.push(layout);
                Ok(())
            };
            let mut work = |units: usize| {
                observed += units;
                Ok(())
            };
            let mut creation = ConnectorReadSchemaCreation::new(
                ValueTypeStorageLifetime::pure(),
                &mut request,
                &mut work,
            );
            let public = publish_read_schema(kind, &borrowed, &schema, &[], &mut creation).unwrap();
            let execution = public.execution_schema().unwrap();
            let source = public.source_schema_facts().unwrap();
            assert!(source.field_facts().is_empty());
            assert_eq!(execution.bindings().len(), original.fields().len());
            for (ordinal, original) in original.fields().iter().enumerate() {
                let field = &public.schema().fields()[ordinal];
                let binding = execution.binding(ordinal).unwrap();
                assert_eq!(field.name(), original.name());
                match row.fields()[ordinal].field_type.as_ref() {
                    Type::Primitive(primitive)
                        if crate::temporal::declaration(primitive).is_some() =>
                    {
                        let declaration = crate::temporal::declaration(primitive).unwrap();
                        assert_eq!(field.data_type(), &declaration.carrier());
                        assert_eq!(binding.value_type().temporal(), Some(declaration));
                    }
                    _ => assert_eq!(field.data_type(), original.data_type()),
                }
                assert_eq!(field.is_nullable(), original.is_nullable());
                assert!(field.metadata().is_empty());
                assert_eq!(
                    binding.logical_type(),
                    system_column_domain(kind, original.name())
                );
                assert_eq!(source.shape().schema().fields()[ordinal], *original);
            }
            drop(creation);
            assert!(!requests.is_empty());
            assert!(observed > 0);
        }
    }
    #[test]
    fn public_catalog_runtime_and_observed_metadata_schemas_match_column_for_column() {
        use crate::iceberg::spec::{FormatVersion, SortOrder, TableMetadataBuilder, Transform};
        use crate::metadata_batch_reader::{MetadataTableType as M, metadata_table_output_columns};
        for partitioned in [false, true] {
            let schema = schema();
            let spec = if partitioned {
                PartitionSpec::builder(Arc::new(schema.clone()))
                    .add_partition_field("a", "a", Transform::Identity)
                    .unwrap()
                    .build()
                    .unwrap()
            } else {
                PartitionSpec::unpartition_spec()
            };
            let metadata = TableMetadataBuilder::new(
                schema.clone(),
                spec.into_unbound(),
                SortOrder::unsorted_order(),
                "file:///public-metadata-schema".into(),
                FormatVersion::V2,
                HashMap::new(),
            )
            .unwrap()
            .build()
            .unwrap()
            .metadata;
            let specs: Vec<_> = metadata
                .partition_specs_iter()
                .map(|spec| spec.as_ref().clone())
                .collect();
            let borrowed: Vec<_> = specs.iter().collect();
            for (public_kind, kind) in [
                (M::Files, IcebergSystemTableType::Files),
                (M::LogicalIcebergMetadata, IcebergSystemTableType::Entries),
                (M::Snapshots, IcebergSystemTableType::Snapshots),
                (M::History, IcebergSystemTableType::History),
                (M::Refs, IcebergSystemTableType::Refs),
                (M::Manifests, IcebergSystemTableType::Manifests),
            ] {
                let mut requests = 0;
                let mut units = 0;
                let mut request = |_| {
                    requests += 1;
                    Ok(())
                };
                let mut work = |n| {
                    units += n;
                    Ok(())
                };
                let mut creation = ConnectorReadSchemaCreation::new(
                    ValueTypeStorageLifetime::pure(),
                    &mut request,
                    &mut work,
                );
                let public =
                    metadata_table_output_columns(public_kind, &metadata, &mut creation).unwrap();
                let observed =
                    columns(kind, metadata.current_schema(), &borrowed, &mut creation).unwrap();
                let runtime =
                    system_relation_schema(kind, metadata.current_schema(), &specs).unwrap();
                assert_eq!(public.len(), observed.len(), "{kind:?}");
                assert_eq!(public.len(), runtime.fields().len(), "{kind:?}");
                for ((public, (name, value)), field) in
                    public.iter().zip(&observed).zip(runtime.fields())
                {
                    assert_eq!(public.name(), name, "{kind:?}");
                    assert_eq!(public.name(), field.name(), "{kind:?}");
                    assert_eq!(public.data_type(), value.data_type(), "{kind:?}: {name}");
                    assert_eq!(public.data_type(), field.data_type(), "{kind:?}: {name}");
                    assert_eq!(public.nullable(), value.nullable(), "{kind:?}: {name}");
                    assert_eq!(public.nullable(), field.is_nullable(), "{kind:?}: {name}");
                    assert_eq!(value.logical_type(), system_column_domain(kind, name));
                }
                drop(creation);
                assert!(requests > 0 && units > 0);
            }
        }
    }

    #[test]
    fn recursive_statistics_keep_qualified_names_nominal_domains_and_temporal_precision() {
        use crate::iceberg::spec::VariantType;
        let schema = Schema::builder()
            .with_fields([
                Arc::new(NestedField::optional(
                    11,
                    "events",
                    Type::List(ListType::new(Arc::new(NestedField::list_element(
                        12,
                        Type::Struct(StructType::new(vec![
                            Arc::new(NestedField::optional(
                                13,
                                "id",
                                Type::Primitive(PrimitiveType::Uuid),
                            )),
                            Arc::new(NestedField::optional(
                                14,
                                "version",
                                Type::Variant(VariantType),
                            )),
                            Arc::new(NestedField::optional(
                                15,
                                "started",
                                Type::Primitive(PrimitiveType::TimestamptzNs),
                            )),
                        ])),
                        true,
                    )))),
                )),
                Arc::new(NestedField::required(
                    6,
                    "attributes",
                    Type::Map(MapType::new(
                        Arc::new(NestedField::map_key_element(
                            7,
                            Type::Primitive(PrimitiveType::String),
                        )),
                        Arc::new(NestedField::map_value_element(
                            8,
                            Type::Primitive(PrimitiveType::Fixed(16)),
                            false,
                        )),
                    )),
                )),
            ])
            .build()
            .unwrap();
        let mut request = |_| Ok(());
        let mut work = |_| Ok(());
        let mut creation = ConnectorReadSchemaCreation::new(
            ValueTypeStorageLifetime::pure(),
            &mut request,
            &mut work,
        );
        let statistics = statistics_fields(&schema, &mut creation).unwrap();
        assert_eq!(
            statistics
                .iter()
                .map(|field| (field.id, field.name.as_str()))
                .collect::<Vec<_>>(),
            [
                (13, "events.element.id"),
                (15, "events.element.started"),
                (7, "attributes.key"),
                (8, "attributes.value")
            ]
        );
        let handles = system_relation_columns(IcebergSystemTableType::Files, &schema, &[]).unwrap();
        let bounds = handles
            .iter()
            .find(|column| column.base_column_identity().name() == "lower_bounds")
            .unwrap();
        let public = publish_read_schema(
            IcebergSystemTableType::Files,
            &[bounds],
            &schema,
            &[],
            &mut creation,
        )
        .unwrap();
        let source = public
            .source_schema_facts()
            .unwrap()
            .shape()
            .field_at(&[0])
            .unwrap();
        let DataType::Struct(source) = source.data_type() else {
            panic!("bounds source must be a ROW");
        };
        assert_eq!(source[0].data_type(), &DataType::FixedSizeBinary(16));
        assert_eq!(
            source[1].data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
        );
        assert_eq!(source[3].data_type(), &DataType::Binary);
        let execution = public.execution_schema().unwrap();
        let value = execution.binding(0).unwrap().value_type();
        let ValueKind::Struct(fields) = value.kind() else {
            panic!("bounds value must be a ROW");
        };
        assert_eq!(
            fields.iter().map(|field| field.name()).collect::<Vec<_>>(),
            ["13", "15", "7", "8"]
        );
        assert!(fields.iter().all(|field| field.value_type().nullable()));
        assert_eq!(
            fields[0].value_type().domain(),
            Some(ValueLogicalType::Uuid)
        );
        assert_eq!(
            fields[1].value_type().temporal(),
            crate::temporal::declaration(&PrimitiveType::TimestamptzNs)
        );
        assert!(matches!(fields[3].value_type().kind(), ValueKind::Binary));
    }

    #[test]
    fn variant_only_schema_omits_files_bounds_but_keeps_entries_text_maps() {
        let schema = Schema::builder()
            .with_fields([Arc::new(NestedField::optional(
                1,
                "payload",
                Type::Variant(crate::iceberg::spec::VariantType),
            ))])
            .build()
            .unwrap();
        let files = system_relation_schema(IcebergSystemTableType::Files, &schema, &[]).unwrap();
        assert!(files.field_with_name("partition").is_err());
        assert!(files.field_with_name("lower_bounds").is_err());
        assert!(files.field_with_name("upper_bounds").is_err());
        assert!(files.field_with_name("readable_metrics").is_ok());
        let entries =
            system_relation_schema(IcebergSystemTableType::Entries, &schema, &[]).unwrap();
        let DataType::Struct(fields) = entries.field_with_name("data_file").unwrap().data_type()
        else {
            panic!("data_file must be a ROW");
        };
        assert!(fields.iter().any(|field| field.name() == "lower_bounds"));
        assert!(fields.iter().any(|field| field.name() == "upper_bounds"));
        assert!(fields.iter().all(|field| field.name() != "partition"));
    }

    #[test]
    fn historical_identity_partition_keeps_exact_uuid_domain_and_renamed_slot() {
        use crate::iceberg::spec::Transform;
        let schema = Schema::builder()
            .with_fields([Arc::new(NestedField::optional(
                1,
                "id",
                Type::Primitive(PrimitiveType::Uuid),
            ))])
            .build()
            .unwrap();
        let old = PartitionSpec::builder(Arc::new(schema.clone()))
            .with_spec_id(2)
            .add_partition_field("id", "old_id", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();
        let new = PartitionSpec::builder(Arc::new(schema.clone()))
            .with_spec_id(3)
            .add_partition_field("id", "current_id", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();
        let specs = [old, new];
        let borrowed = specs.iter().collect::<Vec<_>>();
        let handles =
            system_relation_columns(IcebergSystemTableType::Files, &schema, &specs).unwrap();
        let partition = handles
            .iter()
            .find(|column| column.base_column_identity().name() == "partition")
            .unwrap();
        let mut request = |_| Ok(());
        let mut work = |_| Ok(());
        let mut creation = ConnectorReadSchemaCreation::new(
            ValueTypeStorageLifetime::pure(),
            &mut request,
            &mut work,
        );
        let facts = partition_facts_for_specs(&schema, &borrowed, &mut creation).unwrap();
        assert_eq!(facts.field_ids(), [1000]);
        assert_eq!(facts.slots_by_spec[&2], [Some(0)]);
        assert_eq!(facts.slots_by_spec[&3], [Some(0)]);
        let public = publish_read_schema_from_facts(
            IcebergSystemTableType::Files,
            &[partition],
            &schema,
            &facts,
            &mut creation,
        )
        .unwrap();
        let execution = public.execution_schema().unwrap();
        let value = execution.binding(0).unwrap().value_type();
        let ValueKind::Struct(fields) = value.kind() else {
            panic!("partition must be a ROW");
        };
        assert_eq!(fields[0].name(), "current_id");
        assert_eq!(
            fields[0].value_type().domain(),
            Some(ValueLogicalType::Uuid)
        );
        assert!(fields[0].value_type().nullable());
    }

    #[test]
    fn independent_system_binding_keeps_only_actual_creator_and_zero_projection_stays_empty() {
        let schema = schema();
        let original =
            system_relation_schema(IcebergSystemTableType::Snapshots, &schema, &[]).unwrap();
        let Type::Struct(row) = metadata_relation_row(&original).unwrap() else {
            panic!("row");
        };
        let column = IcebergColumnHandle::base_column(&row.fields()[0]).unwrap();
        let exits = Arc::new(AtomicUsize::new(0));
        let storage = ValueTypeStorageLifetime::held(Arc::new(Hold(exits.clone())));
        let mut request = |_: Layout| Ok(());
        let mut work = |_: usize| Ok(());
        let mut creation = ConnectorReadSchemaCreation::new(storage, &mut request, &mut work);
        let public = publish_read_schema(
            IcebergSystemTableType::Snapshots,
            &[&column],
            &schema,
            &[],
            &mut creation,
        )
        .unwrap();
        let value = public
            .execution_schema()
            .unwrap()
            .binding(0)
            .unwrap()
            .clone();
        let source = public.source_schema_facts().unwrap().shape().clone();
        assert!(std::ptr::eq(
            source.field_at(&[0]).unwrap(),
            public
                .source_schema_facts()
                .unwrap()
                .shape()
                .schema()
                .fields()[0]
                .as_ref(),
        ));
        drop(public);
        drop(creation);
        assert_eq!(exits.load(Ordering::SeqCst), 0);
        let timestamp = crate::temporal::declaration(&PrimitiveType::Timestamptz).unwrap();
        assert_eq!(value.value_type().temporal(), Some(timestamp));
        assert_eq!(value.data_type(), &timestamp.carrier());
        drop(value);
        assert_eq!(exits.load(Ordering::SeqCst), 0);
        assert!(
            matches!(source.field_at(&[0]).unwrap().data_type(),DataType::Timestamp(TimeUnit::Microsecond,Some(tz)) if tz.as_ref()==UTC_TIME_ZONE)
        );
        drop(source);
        assert_eq!(exits.load(Ordering::SeqCst), 1);
        let mut request = |_: Layout| Ok(());
        let mut work = |_: usize| Ok(());
        let mut creation = ConnectorReadSchemaCreation::new(
            ValueTypeStorageLifetime::pure(),
            &mut request,
            &mut work,
        );
        let public = publish_read_schema(
            IcebergSystemTableType::Files,
            &[],
            &schema,
            &[],
            &mut creation,
        )
        .unwrap();
        assert!(public.schema().fields().is_empty());
        assert!(public.execution_schema().unwrap().bindings().is_empty());
        assert!(
            public
                .source_schema_facts()
                .unwrap()
                .shape()
                .schema()
                .fields()
                .is_empty()
        );
    }
}
