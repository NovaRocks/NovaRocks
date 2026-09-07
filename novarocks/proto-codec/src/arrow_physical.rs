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

//! Lossless Arrow physical schema encoding for internal execution relations.
//!
//! This is deliberately separate from the SQL `TypeDesc` codec. SQL types are
//! semantic and normalize several Arrow representations; this carrier freezes
//! names, nullability, metadata, nesting, offset widths, map ordering, and the
//! exact physical type selected by the planner.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{
    DataType, Field, Fields, IntervalUnit, Schema, SchemaRef, TimeUnit, UnionFields, UnionMode,
};
use novarocks_proto_models::plan;
use novarocks_spi::connector::write_stack::{
    MAX_WRITE_RELATION_DECODED_SCHEMA_BYTES, MAX_WRITE_RELATION_FIELD_NAME_BYTES,
    MAX_WRITE_RELATION_METADATA_ENTRIES_PER_FIELD, MAX_WRITE_RELATION_METADATA_KEY_BYTES,
    MAX_WRITE_RELATION_METADATA_VALUE_BYTES, MAX_WRITE_RELATION_TYPE_DEPTH,
    MAX_WRITER_AUXILIARY_CHANNELS, WRITE_RELATION_COLUMN_COUNT,
};

use crate::{FieldPath, ProtocolError, ProtocolErrorKind};

const FIELD_CHARGE: usize = 128;
const TYPE_CHARGE: usize = 64;
const MAX_COLUMNS: usize = WRITE_RELATION_COLUMN_COUNT + MAX_WRITER_AUXILIARY_CHANNELS;

#[derive(Clone, Debug)]
pub struct DecodedArrowPhysicalSchema {
    schema: SchemaRef,
    slot_ids: Vec<u32>,
    internal: Vec<bool>,
}

impl DecodedArrowPhysicalSchema {
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn slot_ids(&self) -> &[u32] {
        &self.slot_ids
    }

    pub fn internal(&self) -> &[bool] {
        &self.internal
    }
}

/// Encode and immediately decode the descriptor before returning it. This
/// makes a lossy or incomplete FE mapping fail where the plan is produced.
pub fn encode_schema(
    schema: &Schema,
    slot_ids: &[u32],
    internal: bool,
    path: FieldPath,
) -> Result<
    (
        Vec<plan::ArrowPhysicalColumn>,
        Vec<plan::ArrowFieldMetadataEntry>,
    ),
    ProtocolError,
> {
    encode_schema_with_internal(schema, slot_ids, &vec![internal; slot_ids.len()], path)
}

/// Encode a schema whose columns have independently frozen visibility bits.
pub fn encode_schema_with_internal(
    schema: &Schema,
    slot_ids: &[u32],
    internal: &[bool],
    path: FieldPath,
) -> Result<
    (
        Vec<plan::ArrowPhysicalColumn>,
        Vec<plan::ArrowFieldMetadataEntry>,
    ),
    ProtocolError,
> {
    if schema.fields().len() != slot_ids.len() {
        return Err(error(
            path.clone(),
            ProtocolErrorKind::InconsistentFields,
            "Arrow schema field count does not match slot-id count",
        ));
    }
    if schema.fields().len() != internal.len() {
        return Err(error(
            path,
            ProtocolErrorKind::InconsistentFields,
            "Arrow schema field count does not match internal-flag count",
        ));
    }
    if schema.fields().len() > MAX_COLUMNS {
        return Err(error(
            path,
            ProtocolErrorKind::Capacity,
            "Arrow schema exceeds the internal relation column limit",
        ));
    }
    let mut budget = DecodeBudget::default();
    budget.charge(
        schema.fields().len().saturating_mul(FIELD_CHARGE),
        path.clone().field("columns"),
    )?;
    preflight_metadata(
        schema.metadata(),
        path.clone().field("schema_metadata"),
        &mut budget,
    )?;
    for (index, field) in schema.fields().iter().enumerate() {
        preflight_field(
            field,
            path.clone().field("columns").index(index).field("field"),
            1,
            &mut budget,
        )?;
    }
    let columns = schema
        .fields()
        .iter()
        .zip(slot_ids.iter().copied())
        .zip(internal.iter().copied())
        .map(
            |((field, slot_id), is_internal)| plan::ArrowPhysicalColumn {
                slot_id,
                field: Some(encode_field(field.as_ref())),
                is_internal,
            },
        )
        .collect::<Vec<_>>();
    let schema_metadata = encode_metadata(schema.metadata());
    let decoded = decode_schema(&columns, &schema_metadata, path.clone())?;
    let round_trip_columns = decoded
        .schema()
        .fields()
        .iter()
        .zip(decoded.slot_ids().iter().copied())
        .zip(decoded.internal().iter().copied())
        .map(
            |((field, slot_id), is_internal)| plan::ArrowPhysicalColumn {
                slot_id,
                field: Some(encode_field(field.as_ref())),
                is_internal,
            },
        )
        .collect::<Vec<_>>();
    let round_trip_metadata = encode_metadata(decoded.schema().metadata());
    if decoded.schema().as_ref() != schema
        || decoded.slot_ids() != slot_ids
        || decoded.internal() != internal
        || round_trip_columns != columns
        || round_trip_metadata != schema_metadata
    {
        return Err(error(
            path,
            ProtocolErrorKind::InconsistentFields,
            "Arrow physical schema failed exact encoder self-validation",
        ));
    }
    Ok((columns, schema_metadata))
}

/// Decode an untrusted physical schema with semantic allocation limits applied
/// before constructing Arrow fields.
pub fn decode_schema(
    columns: &[plan::ArrowPhysicalColumn],
    schema_metadata: &[plan::ArrowFieldMetadataEntry],
    path: FieldPath,
) -> Result<DecodedArrowPhysicalSchema, ProtocolError> {
    if columns.len() > MAX_COLUMNS {
        return Err(error(
            path.clone().field("columns"),
            ProtocolErrorKind::Capacity,
            "Arrow schema exceeds the internal relation column limit",
        ));
    }
    let mut budget = DecodeBudget::default();
    budget.charge(
        columns.len().saturating_mul(FIELD_CHARGE),
        path.clone().field("columns"),
    )?;
    let metadata = decode_metadata(
        schema_metadata,
        path.clone().field("schema_metadata"),
        &mut budget,
    )?;
    let mut fields = Vec::with_capacity(columns.len());
    let mut slot_ids = Vec::with_capacity(columns.len());
    let mut internal = Vec::with_capacity(columns.len());
    for (index, column) in columns.iter().enumerate() {
        let column_path = path.clone().field("columns").index(index);
        let field = column.field.as_ref().ok_or_else(|| {
            error(
                column_path.clone().field("field"),
                ProtocolErrorKind::MissingField,
                "Arrow physical column field is required",
            )
        })?;
        fields.push(Arc::new(decode_field(
            field,
            column_path.clone().field("field"),
            1,
            &mut budget,
        )?));
        slot_ids.push(column.slot_id);
        internal.push(column.is_internal);
    }
    Ok(DecodedArrowPhysicalSchema {
        schema: Arc::new(Schema::new_with_metadata(fields, metadata)),
        slot_ids,
        internal,
    })
}

fn encode_field(field: &Field) -> plan::ArrowPhysicalField {
    #[allow(deprecated)]
    let dictionary_id = field.dict_id();
    plan::ArrowPhysicalField {
        name: field.name().clone(),
        nullable: field.is_nullable(),
        r#type: Some(Box::new(encode_type(field.data_type()))),
        metadata: encode_metadata(field.metadata()),
        dictionary_id,
        dictionary_is_ordered: field.dict_is_ordered(),
    }
}

fn encode_metadata(metadata: &HashMap<String, String>) -> Vec<plan::ArrowFieldMetadataEntry> {
    let mut entries = metadata
        .iter()
        .map(|(key, value)| plan::ArrowFieldMetadataEntry {
            key: key.clone(),
            value: value.clone(),
        })
        .collect::<Vec<_>>();
    entries.sort_unstable_by(|left, right| left.key.cmp(&right.key));
    entries
}

fn preflight_field(
    field: &Field,
    path: FieldPath,
    depth: usize,
    budget: &mut DecodeBudget,
) -> Result<(), ProtocolError> {
    check_depth(depth, path.clone())?;
    check_string(
        field.name(),
        MAX_WRITE_RELATION_FIELD_NAME_BYTES,
        path.clone().field("name"),
        "Arrow field name",
    )?;
    budget.charge(FIELD_CHARGE + field.name().len(), path.clone())?;
    preflight_metadata(field.metadata(), path.clone().field("metadata"), budget)?;
    preflight_type(field.data_type(), path.field("type"), depth, budget)
}

fn preflight_type(
    data_type: &DataType,
    path: FieldPath,
    depth: usize,
    budget: &mut DecodeBudget,
) -> Result<(), ProtocolError> {
    check_depth(depth, path.clone())?;
    budget.charge(TYPE_CHARGE, path.clone())?;
    match data_type {
        DataType::Timestamp(_, Some(timezone)) => {
            check_string(
                timezone,
                MAX_WRITE_RELATION_FIELD_NAME_BYTES,
                path.clone().field("timestamp").field("timezone"),
                "Arrow timestamp timezone",
            )?;
            budget.charge(timezone.len(), path)?;
        }
        DataType::List(field) => preflight_field(field, path.field("list"), depth + 1, budget)?,
        DataType::ListView(field) => {
            preflight_field(field, path.field("list_view"), depth + 1, budget)?
        }
        DataType::FixedSizeList(field, _) => {
            preflight_field(field, path.field("fixed_size_list"), depth + 1, budget)?
        }
        DataType::LargeList(field) => {
            preflight_field(field, path.field("large_list"), depth + 1, budget)?
        }
        DataType::LargeListView(field) => {
            preflight_field(field, path.field("large_list_view"), depth + 1, budget)?
        }
        DataType::Struct(fields) => {
            check_repeated_len(
                fields.len(),
                path.clone().field("struct_type").field("fields"),
            )?;
            for (index, field) in fields.iter().enumerate() {
                preflight_field(
                    field,
                    path.clone()
                        .field("struct_type")
                        .field("fields")
                        .index(index),
                    depth + 1,
                    budget,
                )?;
            }
        }
        DataType::Union(fields, _) => {
            check_repeated_len(
                fields.len(),
                path.clone().field("union_type").field("fields"),
            )?;
            for (index, (_, field)) in fields.iter().enumerate() {
                preflight_field(
                    field,
                    path.clone()
                        .field("union_type")
                        .field("fields")
                        .index(index),
                    depth + 1,
                    budget,
                )?;
            }
        }
        DataType::Dictionary(key, value) => {
            preflight_type(
                key,
                path.clone().field("dictionary").field("key"),
                depth + 1,
                budget,
            )?;
            preflight_type(
                value,
                path.field("dictionary").field("value"),
                depth + 1,
                budget,
            )?;
        }
        DataType::Map(entries, _) => preflight_field(
            entries,
            path.field("map").field("entries"),
            depth + 1,
            budget,
        )?,
        DataType::RunEndEncoded(run_ends, values) => {
            preflight_field(
                run_ends,
                path.clone().field("run_end_encoded").field("run_ends"),
                depth + 1,
                budget,
            )?;
            preflight_field(
                values,
                path.field("run_end_encoded").field("values"),
                depth + 1,
                budget,
            )?;
        }
        _ => {}
    }
    Ok(())
}

fn preflight_metadata(
    metadata: &HashMap<String, String>,
    path: FieldPath,
    budget: &mut DecodeBudget,
) -> Result<(), ProtocolError> {
    if metadata.len() > MAX_WRITE_RELATION_METADATA_ENTRIES_PER_FIELD {
        return Err(error(
            path,
            ProtocolErrorKind::Capacity,
            "Arrow metadata exceeds the entry limit",
        ));
    }
    for (key, value) in metadata {
        check_string(
            key,
            MAX_WRITE_RELATION_METADATA_KEY_BYTES,
            path.clone().field("key"),
            "Arrow metadata key",
        )?;
        check_string(
            value,
            MAX_WRITE_RELATION_METADATA_VALUE_BYTES,
            path.clone().field("value"),
            "Arrow metadata value",
        )?;
        budget.charge(
            key.len() + value.len() + 2 * size_of::<String>(),
            path.clone(),
        )?;
    }
    Ok(())
}

fn encode_type(data_type: &DataType) -> plan::ArrowPhysicalType {
    use plan::arrow_physical_type::Kind;
    let kind = match data_type {
        DataType::Null => primitive(plan::ArrowPrimitiveType::Null),
        DataType::Boolean => primitive(plan::ArrowPrimitiveType::Boolean),
        DataType::Int8 => primitive(plan::ArrowPrimitiveType::Int8),
        DataType::Int16 => primitive(plan::ArrowPrimitiveType::Int16),
        DataType::Int32 => primitive(plan::ArrowPrimitiveType::Int32),
        DataType::Int64 => primitive(plan::ArrowPrimitiveType::Int64),
        DataType::UInt8 => primitive(plan::ArrowPrimitiveType::Uint8),
        DataType::UInt16 => primitive(plan::ArrowPrimitiveType::Uint16),
        DataType::UInt32 => primitive(plan::ArrowPrimitiveType::Uint32),
        DataType::UInt64 => primitive(plan::ArrowPrimitiveType::Uint64),
        DataType::Float16 => primitive(plan::ArrowPrimitiveType::Float16),
        DataType::Float32 => primitive(plan::ArrowPrimitiveType::Float32),
        DataType::Float64 => primitive(plan::ArrowPrimitiveType::Float64),
        DataType::Date32 => primitive(plan::ArrowPrimitiveType::Date32),
        DataType::Date64 => primitive(plan::ArrowPrimitiveType::Date64),
        DataType::Binary => primitive(plan::ArrowPrimitiveType::Binary),
        DataType::BinaryView => primitive(plan::ArrowPrimitiveType::BinaryView),
        DataType::LargeBinary => primitive(plan::ArrowPrimitiveType::LargeBinary),
        DataType::Utf8 => primitive(plan::ArrowPrimitiveType::Utf8),
        DataType::Utf8View => primitive(plan::ArrowPrimitiveType::Utf8View),
        DataType::LargeUtf8 => primitive(plan::ArrowPrimitiveType::LargeUtf8),
        DataType::Timestamp(unit, timezone) => Kind::Timestamp(plan::ArrowTimestampType {
            unit: encode_time_unit(*unit) as i32,
            timezone: timezone.as_ref().map(|value| value.to_string()),
        }),
        DataType::Time32(unit) => Kind::Time32(plan::ArrowTimeType {
            unit: encode_time_unit(*unit) as i32,
        }),
        DataType::Time64(unit) => Kind::Time64(plan::ArrowTimeType {
            unit: encode_time_unit(*unit) as i32,
        }),
        DataType::Duration(unit) => Kind::Duration(plan::ArrowTimeType {
            unit: encode_time_unit(*unit) as i32,
        }),
        DataType::Interval(unit) => Kind::Interval(encode_interval_unit(*unit) as i32),
        DataType::FixedSizeBinary(width) => Kind::FixedSizeBinary(*width),
        DataType::Decimal32(precision, scale) => Kind::Decimal32(decimal(*precision, *scale)),
        DataType::Decimal64(precision, scale) => Kind::Decimal64(decimal(*precision, *scale)),
        DataType::Decimal128(precision, scale) => Kind::Decimal128(decimal(*precision, *scale)),
        DataType::Decimal256(precision, scale) => Kind::Decimal256(decimal(*precision, *scale)),
        DataType::List(field) => Kind::List(Box::new(encode_field(field))),
        DataType::ListView(field) => Kind::ListView(Box::new(encode_field(field))),
        DataType::FixedSizeList(field, length) => {
            Kind::FixedSizeList(Box::new(plan::ArrowFixedSizeListType {
                item: Some(Box::new(encode_field(field))),
                length: *length,
            }))
        }
        DataType::LargeList(field) => Kind::LargeList(Box::new(encode_field(field))),
        DataType::LargeListView(field) => Kind::LargeListView(Box::new(encode_field(field))),
        DataType::Struct(fields) => Kind::StructType(plan::ArrowStructType {
            fields: fields.iter().map(|field| encode_field(field)).collect(),
        }),
        DataType::Union(fields, mode) => Kind::UnionType(plan::ArrowUnionType {
            mode: match mode {
                UnionMode::Sparse => plan::ArrowUnionMode::Sparse as i32,
                UnionMode::Dense => plan::ArrowUnionMode::Dense as i32,
            },
            fields: fields
                .iter()
                .map(|(type_id, field)| plan::ArrowUnionField {
                    type_id: i32::from(type_id),
                    field: Some(encode_field(field)),
                })
                .collect(),
        }),
        DataType::Dictionary(key, value) => Kind::Dictionary(Box::new(plan::ArrowDictionaryType {
            key: Some(Box::new(encode_type(key))),
            value: Some(Box::new(encode_type(value))),
        })),
        DataType::Map(entries, ordered) => Kind::Map(Box::new(plan::ArrowMapType {
            entries: Some(Box::new(encode_field(entries))),
            ordered: *ordered,
        })),
        DataType::RunEndEncoded(run_ends, values) => {
            Kind::RunEndEncoded(Box::new(plan::ArrowRunEndEncodedType {
                run_ends: Some(Box::new(encode_field(run_ends))),
                values: Some(Box::new(encode_field(values))),
            }))
        }
    };
    plan::ArrowPhysicalType { kind: Some(kind) }
}

fn primitive(value: plan::ArrowPrimitiveType) -> plan::arrow_physical_type::Kind {
    plan::arrow_physical_type::Kind::Primitive(value as i32)
}

fn decimal(precision: u8, scale: i8) -> plan::ArrowDecimalType {
    plan::ArrowDecimalType {
        precision: u32::from(precision),
        scale: i32::from(scale),
    }
}

fn encode_time_unit(unit: TimeUnit) -> plan::ArrowTimeUnit {
    match unit {
        TimeUnit::Second => plan::ArrowTimeUnit::Second,
        TimeUnit::Millisecond => plan::ArrowTimeUnit::Millisecond,
        TimeUnit::Microsecond => plan::ArrowTimeUnit::Microsecond,
        TimeUnit::Nanosecond => plan::ArrowTimeUnit::Nanosecond,
    }
}

fn encode_interval_unit(unit: IntervalUnit) -> plan::ArrowIntervalUnit {
    match unit {
        IntervalUnit::YearMonth => plan::ArrowIntervalUnit::YearMonth,
        IntervalUnit::DayTime => plan::ArrowIntervalUnit::DayTime,
        IntervalUnit::MonthDayNano => plan::ArrowIntervalUnit::MonthDayNano,
    }
}

fn decode_field(
    field: &plan::ArrowPhysicalField,
    path: FieldPath,
    depth: usize,
    budget: &mut DecodeBudget,
) -> Result<Field, ProtocolError> {
    check_depth(depth, path.clone())?;
    check_string(
        &field.name,
        MAX_WRITE_RELATION_FIELD_NAME_BYTES,
        path.clone().field("name"),
        "Arrow field name",
    )?;
    budget.charge(FIELD_CHARGE + field.name.len(), path.clone())?;
    let metadata = decode_metadata(&field.metadata, path.clone().field("metadata"), budget)?;
    let data_type = field.r#type.as_ref().ok_or_else(|| {
        error(
            path.clone().field("type"),
            ProtocolErrorKind::MissingField,
            "Arrow physical field type is required",
        )
    })?;
    let data_type = decode_type(data_type, path.clone().field("type"), depth, budget)?;
    let decoded = if matches!(data_type, DataType::Dictionary(_, _)) {
        let dictionary_id = field.dictionary_id.ok_or_else(|| {
            error(
                path.clone().field("dictionary_id"),
                ProtocolErrorKind::MissingField,
                "Arrow dictionary field id is required",
            )
        })?;
        let dictionary_is_ordered = field.dictionary_is_ordered.ok_or_else(|| {
            error(
                path.clone().field("dictionary_is_ordered"),
                ProtocolErrorKind::MissingField,
                "Arrow dictionary field ordering is required",
            )
        })?;
        #[allow(deprecated)]
        Field::new_dict(
            &field.name,
            data_type,
            field.nullable,
            dictionary_id,
            dictionary_is_ordered,
        )
    } else {
        if field.dictionary_id.is_some() || field.dictionary_is_ordered.is_some() {
            return Err(error(
                path.clone(),
                ProtocolErrorKind::InconsistentFields,
                "Arrow dictionary field attributes require Dictionary type",
            ));
        }
        Field::new(&field.name, data_type, field.nullable)
    };
    Ok(decoded.with_metadata(metadata))
}

fn decode_type(
    data_type: &plan::ArrowPhysicalType,
    path: FieldPath,
    depth: usize,
    budget: &mut DecodeBudget,
) -> Result<DataType, ProtocolError> {
    check_depth(depth, path.clone())?;
    budget.charge(TYPE_CHARGE, path.clone())?;
    use plan::arrow_physical_type::Kind;
    let kind = data_type.kind.as_ref().ok_or_else(|| {
        error(
            path.clone().field("kind"),
            ProtocolErrorKind::MissingField,
            "Arrow physical type kind is required",
        )
    })?;
    match kind {
        Kind::Primitive(value) => decode_primitive(*value, path.field("primitive")),
        Kind::Timestamp(value) => {
            if let Some(timezone) = &value.timezone {
                check_string(
                    timezone,
                    MAX_WRITE_RELATION_FIELD_NAME_BYTES,
                    path.clone().field("timestamp").field("timezone"),
                    "Arrow timestamp timezone",
                )?;
                budget.charge(timezone.len(), path.clone())?;
            }
            Ok(DataType::Timestamp(
                decode_time_unit(value.unit, path.clone().field("timestamp").field("unit"))?,
                value.timezone.as_deref().map(Arc::<str>::from),
            ))
        }
        Kind::Time32(value) => Ok(DataType::Time32(decode_time_unit(
            value.unit,
            path.clone().field("time32").field("unit"),
        )?)),
        Kind::Time64(value) => Ok(DataType::Time64(decode_time_unit(
            value.unit,
            path.clone().field("time64").field("unit"),
        )?)),
        Kind::Duration(value) => Ok(DataType::Duration(decode_time_unit(
            value.unit,
            path.clone().field("duration").field("unit"),
        )?)),
        Kind::Interval(value) => Ok(DataType::Interval(decode_interval_unit(
            *value,
            path.field("interval"),
        )?)),
        Kind::FixedSizeBinary(width) => Ok(DataType::FixedSizeBinary(*width)),
        Kind::Decimal32(value) => {
            decode_decimal(value, path.field("decimal32"), DataType::Decimal32)
        }
        Kind::Decimal64(value) => {
            decode_decimal(value, path.field("decimal64"), DataType::Decimal64)
        }
        Kind::Decimal128(value) => {
            decode_decimal(value, path.field("decimal128"), DataType::Decimal128)
        }
        Kind::Decimal256(value) => {
            decode_decimal(value, path.field("decimal256"), DataType::Decimal256)
        }
        Kind::List(field) => Ok(DataType::List(Arc::new(decode_field(
            field,
            path.field("list"),
            depth + 1,
            budget,
        )?))),
        Kind::ListView(field) => Ok(DataType::ListView(Arc::new(decode_field(
            field,
            path.field("list_view"),
            depth + 1,
            budget,
        )?))),
        Kind::FixedSizeList(value) => {
            let item = value.item.as_ref().ok_or_else(|| {
                error(
                    path.clone().field("fixed_size_list").field("item"),
                    ProtocolErrorKind::MissingField,
                    "Arrow fixed-size list item is required",
                )
            })?;
            Ok(DataType::FixedSizeList(
                Arc::new(decode_field(
                    item,
                    path.field("fixed_size_list").field("item"),
                    depth + 1,
                    budget,
                )?),
                value.length,
            ))
        }
        Kind::LargeList(field) => Ok(DataType::LargeList(Arc::new(decode_field(
            field,
            path.field("large_list"),
            depth + 1,
            budget,
        )?))),
        Kind::LargeListView(field) => Ok(DataType::LargeListView(Arc::new(decode_field(
            field,
            path.field("large_list_view"),
            depth + 1,
            budget,
        )?))),
        Kind::StructType(value) => {
            check_repeated_len(
                value.fields.len(),
                path.clone().field("struct_type").field("fields"),
            )?;
            let fields = value
                .fields
                .iter()
                .enumerate()
                .map(|(index, field)| {
                    decode_field(
                        field,
                        path.clone()
                            .field("struct_type")
                            .field("fields")
                            .index(index),
                        depth + 1,
                        budget,
                    )
                    .map(Arc::new)
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(DataType::Struct(Fields::from(fields)))
        }
        Kind::UnionType(value) => {
            check_repeated_len(
                value.fields.len(),
                path.clone().field("union_type").field("fields"),
            )?;
            let mode = match plan::ArrowUnionMode::try_from(value.mode) {
                Ok(plan::ArrowUnionMode::Sparse) => UnionMode::Sparse,
                Ok(plan::ArrowUnionMode::Dense) => UnionMode::Dense,
                Ok(plan::ArrowUnionMode::Unspecified) | Err(_) => {
                    return Err(error(
                        path.clone().field("union_type").field("mode"),
                        ProtocolErrorKind::InvalidEnum,
                        "Arrow union mode is unknown or unspecified",
                    ));
                }
            };
            let mut type_ids = Vec::with_capacity(value.fields.len());
            let mut fields = Vec::with_capacity(value.fields.len());
            for (index, union_field) in value.fields.iter().enumerate() {
                let field_path = path
                    .clone()
                    .field("union_type")
                    .field("fields")
                    .index(index);
                let type_id = i8::try_from(union_field.type_id).map_err(|_| {
                    error(
                        field_path.clone().field("type_id"),
                        ProtocolErrorKind::OutOfRange,
                        "Arrow union type id does not fit i8",
                    )
                })?;
                let field = union_field.field.as_ref().ok_or_else(|| {
                    error(
                        field_path.clone().field("field"),
                        ProtocolErrorKind::MissingField,
                        "Arrow union field is required",
                    )
                })?;
                type_ids.push(type_id);
                fields.push(Arc::new(decode_field(
                    field,
                    field_path.field("field"),
                    depth + 1,
                    budget,
                )?));
            }
            let fields = UnionFields::try_new(type_ids, fields).map_err(|err| {
                error(
                    path.clone().field("union_type").field("fields"),
                    ProtocolErrorKind::InvalidValue,
                    format!("invalid Arrow union fields: {err}"),
                )
            })?;
            Ok(DataType::Union(fields, mode))
        }
        Kind::Dictionary(value) => {
            let key = value.key.as_ref().ok_or_else(|| {
                error(
                    path.clone().field("dictionary").field("key"),
                    ProtocolErrorKind::MissingField,
                    "Arrow dictionary key type is required",
                )
            })?;
            let dictionary_value = value.value.as_ref().ok_or_else(|| {
                error(
                    path.clone().field("dictionary").field("value"),
                    ProtocolErrorKind::MissingField,
                    "Arrow dictionary value type is required",
                )
            })?;
            Ok(DataType::Dictionary(
                Box::new(decode_type(
                    key,
                    path.clone().field("dictionary").field("key"),
                    depth + 1,
                    budget,
                )?),
                Box::new(decode_type(
                    dictionary_value,
                    path.field("dictionary").field("value"),
                    depth + 1,
                    budget,
                )?),
            ))
        }
        Kind::Map(value) => {
            let entries = value.entries.as_ref().ok_or_else(|| {
                error(
                    path.clone().field("map").field("entries"),
                    ProtocolErrorKind::MissingField,
                    "Arrow map entries field is required",
                )
            })?;
            Ok(DataType::Map(
                Arc::new(decode_field(
                    entries,
                    path.field("map").field("entries"),
                    depth + 1,
                    budget,
                )?),
                value.ordered,
            ))
        }
        Kind::RunEndEncoded(value) => {
            let run_ends = value.run_ends.as_ref().ok_or_else(|| {
                error(
                    path.clone().field("run_end_encoded").field("run_ends"),
                    ProtocolErrorKind::MissingField,
                    "Arrow run-end field is required",
                )
            })?;
            let values = value.values.as_ref().ok_or_else(|| {
                error(
                    path.clone().field("run_end_encoded").field("values"),
                    ProtocolErrorKind::MissingField,
                    "Arrow run-end values field is required",
                )
            })?;
            Ok(DataType::RunEndEncoded(
                Arc::new(decode_field(
                    run_ends,
                    path.clone().field("run_end_encoded").field("run_ends"),
                    depth + 1,
                    budget,
                )?),
                Arc::new(decode_field(
                    values,
                    path.field("run_end_encoded").field("values"),
                    depth + 1,
                    budget,
                )?),
            ))
        }
    }
}

fn decode_primitive(value: i32, path: FieldPath) -> Result<DataType, ProtocolError> {
    match plan::ArrowPrimitiveType::try_from(value) {
        Ok(plan::ArrowPrimitiveType::Null) => Ok(DataType::Null),
        Ok(plan::ArrowPrimitiveType::Boolean) => Ok(DataType::Boolean),
        Ok(plan::ArrowPrimitiveType::Int8) => Ok(DataType::Int8),
        Ok(plan::ArrowPrimitiveType::Int16) => Ok(DataType::Int16),
        Ok(plan::ArrowPrimitiveType::Int32) => Ok(DataType::Int32),
        Ok(plan::ArrowPrimitiveType::Int64) => Ok(DataType::Int64),
        Ok(plan::ArrowPrimitiveType::Uint8) => Ok(DataType::UInt8),
        Ok(plan::ArrowPrimitiveType::Uint16) => Ok(DataType::UInt16),
        Ok(plan::ArrowPrimitiveType::Uint32) => Ok(DataType::UInt32),
        Ok(plan::ArrowPrimitiveType::Uint64) => Ok(DataType::UInt64),
        Ok(plan::ArrowPrimitiveType::Float16) => Ok(DataType::Float16),
        Ok(plan::ArrowPrimitiveType::Float32) => Ok(DataType::Float32),
        Ok(plan::ArrowPrimitiveType::Float64) => Ok(DataType::Float64),
        Ok(plan::ArrowPrimitiveType::Date32) => Ok(DataType::Date32),
        Ok(plan::ArrowPrimitiveType::Date64) => Ok(DataType::Date64),
        Ok(plan::ArrowPrimitiveType::Binary) => Ok(DataType::Binary),
        Ok(plan::ArrowPrimitiveType::BinaryView) => Ok(DataType::BinaryView),
        Ok(plan::ArrowPrimitiveType::LargeBinary) => Ok(DataType::LargeBinary),
        Ok(plan::ArrowPrimitiveType::Utf8) => Ok(DataType::Utf8),
        Ok(plan::ArrowPrimitiveType::Utf8View) => Ok(DataType::Utf8View),
        Ok(plan::ArrowPrimitiveType::LargeUtf8) => Ok(DataType::LargeUtf8),
        Ok(plan::ArrowPrimitiveType::Unspecified) | Err(_) => Err(error(
            path,
            ProtocolErrorKind::InvalidEnum,
            "Arrow primitive type is unknown or unspecified",
        )),
    }
}

fn decode_time_unit(value: i32, path: FieldPath) -> Result<TimeUnit, ProtocolError> {
    match plan::ArrowTimeUnit::try_from(value) {
        Ok(plan::ArrowTimeUnit::Second) => Ok(TimeUnit::Second),
        Ok(plan::ArrowTimeUnit::Millisecond) => Ok(TimeUnit::Millisecond),
        Ok(plan::ArrowTimeUnit::Microsecond) => Ok(TimeUnit::Microsecond),
        Ok(plan::ArrowTimeUnit::Nanosecond) => Ok(TimeUnit::Nanosecond),
        Ok(plan::ArrowTimeUnit::Unspecified) | Err(_) => Err(error(
            path,
            ProtocolErrorKind::InvalidEnum,
            "Arrow time unit is unknown or unspecified",
        )),
    }
}

fn decode_interval_unit(value: i32, path: FieldPath) -> Result<IntervalUnit, ProtocolError> {
    match plan::ArrowIntervalUnit::try_from(value) {
        Ok(plan::ArrowIntervalUnit::YearMonth) => Ok(IntervalUnit::YearMonth),
        Ok(plan::ArrowIntervalUnit::DayTime) => Ok(IntervalUnit::DayTime),
        Ok(plan::ArrowIntervalUnit::MonthDayNano) => Ok(IntervalUnit::MonthDayNano),
        Ok(plan::ArrowIntervalUnit::Unspecified) | Err(_) => Err(error(
            path,
            ProtocolErrorKind::InvalidEnum,
            "Arrow interval unit is unknown or unspecified",
        )),
    }
}

fn decode_decimal(
    value: &plan::ArrowDecimalType,
    path: FieldPath,
    build: fn(u8, i8) -> DataType,
) -> Result<DataType, ProtocolError> {
    let precision = u8::try_from(value.precision).map_err(|_| {
        error(
            path.clone().field("precision"),
            ProtocolErrorKind::OutOfRange,
            "Arrow decimal precision does not fit u8",
        )
    })?;
    let scale = i8::try_from(value.scale).map_err(|_| {
        error(
            path.field("scale"),
            ProtocolErrorKind::OutOfRange,
            "Arrow decimal scale does not fit i8",
        )
    })?;
    Ok(build(precision, scale))
}

fn decode_metadata(
    entries: &[plan::ArrowFieldMetadataEntry],
    path: FieldPath,
    budget: &mut DecodeBudget,
) -> Result<HashMap<String, String>, ProtocolError> {
    if entries.len() > MAX_WRITE_RELATION_METADATA_ENTRIES_PER_FIELD {
        return Err(error(
            path,
            ProtocolErrorKind::Capacity,
            "Arrow metadata exceeds the entry limit",
        ));
    }
    let mut previous: Option<&str> = None;
    for (index, entry) in entries.iter().enumerate() {
        let entry_path = path.clone().index(index);
        check_string(
            &entry.key,
            MAX_WRITE_RELATION_METADATA_KEY_BYTES,
            entry_path.clone().field("key"),
            "Arrow metadata key",
        )?;
        check_string(
            &entry.value,
            MAX_WRITE_RELATION_METADATA_VALUE_BYTES,
            entry_path.clone().field("value"),
            "Arrow metadata value",
        )?;
        if previous.is_some_and(|key| key >= entry.key.as_str()) {
            return Err(error(
                entry_path.field("key"),
                if previous == Some(entry.key.as_str()) {
                    ProtocolErrorKind::DuplicateField
                } else {
                    ProtocolErrorKind::InvalidValue
                },
                "Arrow metadata keys must be unique and strictly sorted",
            ));
        }
        budget.charge(
            entry.key.len() + entry.value.len() + 2 * size_of::<String>(),
            entry_path,
        )?;
        previous = Some(&entry.key);
    }
    Ok(entries
        .iter()
        .map(|entry| (entry.key.clone(), entry.value.clone()))
        .collect())
}

fn check_repeated_len(length: usize, path: FieldPath) -> Result<(), ProtocolError> {
    if length > MAX_COLUMNS {
        Err(error(
            path,
            ProtocolErrorKind::Capacity,
            "Arrow nested field count exceeds the relation limit",
        ))
    } else {
        Ok(())
    }
}

fn check_depth(depth: usize, path: FieldPath) -> Result<(), ProtocolError> {
    if depth > MAX_WRITE_RELATION_TYPE_DEPTH {
        Err(error(
            path,
            ProtocolErrorKind::Capacity,
            "Arrow type exceeds the nesting depth limit",
        ))
    } else {
        Ok(())
    }
}

fn check_string(
    value: &str,
    limit: usize,
    path: FieldPath,
    label: &'static str,
) -> Result<(), ProtocolError> {
    if value.len() > limit {
        Err(error(
            path,
            ProtocolErrorKind::Capacity,
            format!("{label} exceeds the byte limit"),
        ))
    } else {
        Ok(())
    }
}

#[derive(Default)]
struct DecodeBudget {
    bytes: usize,
}

impl DecodeBudget {
    fn charge(&mut self, amount: usize, path: FieldPath) -> Result<(), ProtocolError> {
        self.bytes = self.bytes.checked_add(amount).ok_or_else(|| {
            error(
                path.clone(),
                ProtocolErrorKind::Capacity,
                "Arrow schema decoded allocation charge overflowed",
            )
        })?;
        if self.bytes > MAX_WRITE_RELATION_DECODED_SCHEMA_BYTES {
            return Err(error(
                path,
                ProtocolErrorKind::Capacity,
                "Arrow schema exceeds the decoded allocation limit",
            ));
        }
        Ok(())
    }
}

fn error(path: FieldPath, kind: ProtocolErrorKind, detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(path, kind, detail)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nested_field(name: &str, data_type: DataType, nullable: bool) -> Arc<Field> {
        Arc::new(
            Field::new(name, data_type, nullable)
                .with_metadata(HashMap::from([("owner".to_string(), "test".to_string())])),
        )
    }

    #[test]
    fn exact_physical_schema_round_trips_without_sql_normalization() {
        let map_entries = nested_field(
            "kv",
            DataType::Struct(Fields::from(vec![
                nested_field("key", DataType::LargeUtf8, false),
                nested_field("value", DataType::LargeBinary, false),
            ])),
            false,
        );
        let schema = Schema::new_with_metadata(
            vec![
                Field::new(
                    "list32",
                    DataType::List(nested_field("element32", DataType::Utf8, false)),
                    true,
                ),
                Field::new(
                    "list64",
                    DataType::LargeList(nested_field("element64", DataType::LargeUtf8, true)),
                    false,
                ),
                Field::new(
                    "fixed",
                    DataType::FixedSizeList(nested_field("fixed_item", DataType::Binary, false), 7),
                    true,
                ),
                Field::new("map", DataType::Map(map_entries, true), true),
                Field::new(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Nanosecond, Some("+08:00".into())),
                    false,
                ),
                #[allow(deprecated)]
                Field::new_dict(
                    "dictionary",
                    DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::LargeUtf8)),
                    false,
                    41,
                    true,
                ),
            ],
            HashMap::from([("schema".to_string(), "physical".to_string())]),
        );
        let slot_ids = [1, 2, 3, 4, 5, 6];
        let path = FieldPath::root("schema");
        let (columns, metadata) = encode_schema(&schema, &slot_ids, true, path.clone()).unwrap();
        let decoded = decode_schema(&columns, &metadata, path).unwrap();
        assert_eq!(decoded.schema().as_ref(), &schema);
        assert_eq!(decoded.slot_ids(), slot_ids);
        assert_eq!(decoded.internal(), &[true; 6]);
        #[allow(deprecated)]
        {
            assert_eq!(decoded.schema().field(5).dict_id(), Some(41));
        }
        assert_eq!(decoded.schema().field(5).dict_is_ordered(), Some(true));
    }

    #[test]
    fn every_arrow_58_physical_variant_round_trips_exactly() {
        let item = || nested_field("item", DataType::Int32, false);
        let union_fields = UnionFields::try_new(
            [1, 7],
            [
                nested_field("left", DataType::Int64, true),
                nested_field("right", DataType::LargeUtf8, false),
            ],
        )
        .expect("union fields");
        let map_entries = nested_field(
            "entries",
            DataType::Struct(Fields::from(vec![
                nested_field("key", DataType::Utf8, false),
                nested_field("value", DataType::Binary, false),
            ])),
            false,
        );
        let data_types = vec![
            DataType::Null,
            DataType::Boolean,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
            DataType::Float16,
            DataType::Float32,
            DataType::Float64,
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            DataType::Date32,
            DataType::Date64,
            DataType::Time32(TimeUnit::Second),
            DataType::Time64(TimeUnit::Nanosecond),
            DataType::Duration(TimeUnit::Millisecond),
            DataType::Interval(IntervalUnit::YearMonth),
            DataType::Interval(IntervalUnit::DayTime),
            DataType::Interval(IntervalUnit::MonthDayNano),
            DataType::Binary,
            DataType::FixedSizeBinary(17),
            DataType::LargeBinary,
            DataType::BinaryView,
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::Utf8View,
            DataType::List(item()),
            DataType::ListView(item()),
            DataType::FixedSizeList(item(), 5),
            DataType::LargeList(item()),
            DataType::LargeListView(item()),
            DataType::Struct(Fields::from(vec![item()])),
            DataType::Union(union_fields, UnionMode::Dense),
            DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::LargeUtf8)),
            DataType::Decimal32(7, -2),
            DataType::Decimal64(16, 3),
            DataType::Decimal128(38, 8),
            DataType::Decimal256(76, 12),
            DataType::Map(map_entries, true),
            DataType::RunEndEncoded(
                nested_field("run_ends", DataType::Int32, false),
                nested_field("values", DataType::Utf8, true),
            ),
        ];
        let fields = data_types
            .into_iter()
            .enumerate()
            .map(|(index, data_type)| Field::new(format!("c{index}"), data_type, index % 2 == 0))
            .collect::<Vec<_>>();
        let schema = Schema::new(fields);
        let slot_ids = (0..schema.fields().len() as u32).collect::<Vec<_>>();
        let path = FieldPath::root("schema");
        let (columns, metadata) = encode_schema(&schema, &slot_ids, false, path.clone()).unwrap();
        let decoded = decode_schema(&columns, &metadata, path).unwrap();
        assert_eq!(decoded.schema().as_ref(), &schema);
        assert_eq!(decoded.slot_ids(), slot_ids);
        assert!(decoded.internal().iter().all(|internal| !internal));
    }

    #[test]
    fn mixed_internal_flags_and_nested_nullability_round_trip_exactly() {
        let schema = Schema::new(vec![
            Field::new(
                "field_ids",
                DataType::List(nested_field("item", DataType::Int32, false)),
                false,
            ),
            Field::new("value", DataType::Binary, false),
        ]);
        let path = FieldPath::root("schema");
        let (columns, metadata) =
            encode_schema_with_internal(&schema, &[10, 11], &[true, false], path.clone()).unwrap();
        let decoded = decode_schema(&columns, &metadata, path).unwrap();
        assert_eq!(decoded.schema().as_ref(), &schema);
        assert_eq!(decoded.internal(), &[true, false]);
    }

    #[test]
    fn decoder_rejects_missing_type_duplicate_metadata_and_excessive_depth() {
        let path = FieldPath::root("schema");
        let schema = Schema::new(vec![Field::new("x", DataType::Int32, false)]);
        let (mut columns, metadata) = encode_schema(&schema, &[1], true, path.clone()).unwrap();
        columns[0].field.as_mut().unwrap().r#type = None;
        assert_eq!(
            decode_schema(&columns, &metadata, path.clone())
                .unwrap_err()
                .kind(),
            ProtocolErrorKind::MissingField
        );

        let (mut columns, metadata) = encode_schema(&schema, &[1], true, path.clone()).unwrap();
        let field = columns[0].field.as_mut().unwrap();
        field.metadata = vec![
            plan::ArrowFieldMetadataEntry {
                key: "a".into(),
                value: "1".into(),
            },
            plan::ArrowFieldMetadataEntry {
                key: "a".into(),
                value: "2".into(),
            },
        ];
        assert_eq!(
            decode_schema(&columns, &metadata, path.clone())
                .unwrap_err()
                .kind(),
            ProtocolErrorKind::DuplicateField
        );

        let dictionary = Schema::new(vec![Field::new(
            "dictionary",
            DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8)),
            false,
        )]);
        let (mut columns, metadata) = encode_schema(&dictionary, &[1], true, path.clone()).unwrap();
        columns[0]
            .field
            .as_mut()
            .expect("field")
            .dictionary_is_ordered = None;
        assert_eq!(
            decode_schema(&columns, &metadata, path.clone())
                .unwrap_err()
                .kind(),
            ProtocolErrorKind::MissingField
        );

        let (mut columns, metadata) = encode_schema(&schema, &[1], true, path.clone()).unwrap();
        columns[0].field.as_mut().expect("field").dictionary_id = Some(7);
        assert_eq!(
            decode_schema(&columns, &metadata, path.clone())
                .unwrap_err()
                .kind(),
            ProtocolErrorKind::InconsistentFields
        );

        let mut nested = DataType::Int32;
        for depth in 0..MAX_WRITE_RELATION_TYPE_DEPTH {
            nested = DataType::List(Arc::new(Field::new(format!("d{depth}"), nested, false)));
        }
        let schema = Schema::new(vec![Field::new("too_deep", nested, false)]);
        assert_eq!(
            encode_schema(&schema, &[1], true, path).unwrap_err().kind(),
            ProtocolErrorKind::Capacity
        );
    }
}
