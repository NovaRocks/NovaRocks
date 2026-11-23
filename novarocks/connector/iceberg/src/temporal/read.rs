// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use std::{collections::HashSet, ops::Range, sync::Arc};

use arrow::array::{
    Array, ArrayRef, FixedSizeBinaryBuilder, Float32Array, Float64Array, ListArray, MapArray,
    StructArray, Time64MicrosecondArray, TimestampMicrosecondArray, TimestampNanosecondArray,
    make_array,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, FieldRef, Fields, TimeUnit};
use arrow::row::{RowConverter, SortField};
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
use novarocks_type_contract::{TemporalCarrier, TemporalType};
use novarocks_value_arrays::{
    decode::PreparedTemporalArray, reachability::SelectionSet, visit_reachable_child_rows_observed,
};

use crate::iceberg::spec::Type;

type ConvertTemporalLeaf<C> = fn(
    &ArrayRef,
    TemporalType,
    &str,
    Option<&SelectionSet>,
    &mut C,
) -> Result<ArrayRef, ConnectorError>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum TemporalReadProjection {
    Identity,
    Leaf(TemporalType),
    Struct(Box<[Self]>),
    List(Box<Self>),
    Map {
        entries: Box<Self>,
        check_keys: bool,
    },
}

impl TemporalReadProjection {
    pub(crate) fn from_type(ty: &Type) -> Self {
        match ty {
            Type::Primitive(primitive) => match super::declaration(primitive) {
                Some(TemporalType::Timestamp(precision)) if precision.fits_microseconds() => {
                    Self::Identity
                }
                Some(declaration) => Self::Leaf(declaration),
                None => Self::Identity,
            },
            Type::Struct(fields) => Self::fields(
                fields
                    .fields()
                    .iter()
                    .map(|field| Self::from_type(&field.field_type)),
            ),
            Type::List(list) => {
                let element = Self::from_type(&list.element_field.field_type);
                if element.is_identity() {
                    Self::Identity
                } else {
                    Self::List(Box::new(element))
                }
            }
            Type::Map(map) => {
                let entries = Self::fields(
                    [&map.key_field, &map.value_field]
                        .into_iter()
                        .map(|field| Self::from_type(&field.field_type)),
                );
                let check_keys = contains_temporal(&map.key_field.field_type);
                if entries.is_identity() && !check_keys {
                    Self::Identity
                } else {
                    Self::Map {
                        entries: Box::new(entries),
                        check_keys,
                    }
                }
            }
            Type::Variant(_) => Self::Identity,
        }
    }

    fn fields(children: impl Iterator<Item = Self>) -> Self {
        let children: Box<[Self]> = children.collect();
        if children.iter().all(Self::is_identity) {
            Self::Identity
        } else {
            Self::Struct(children)
        }
    }

    pub(crate) fn is_identity(&self) -> bool {
        matches!(self, Self::Identity)
    }

    pub(crate) fn needs_read_conversion(&self) -> bool {
        match self {
            Self::Identity => false,
            Self::Leaf(_) => true,
            Self::Struct(children) => children.iter().any(Self::needs_read_conversion),
            Self::List(child) | Self::Map { entries: child, .. } => child.needs_read_conversion(),
        }
    }

    /// Bound the canonical buffers built for this physical span. A rebased
    /// LIST/MAP may allocate a new offset buffer; an unchanged leaf aliases.
    pub(crate) fn additional_read_bytes(
        &self,
        input: &ArrayRef,
        rows: Range<usize>,
        path: &str,
        check_active: &mut impl FnMut() -> Result<(), ConnectorError>,
    ) -> Result<u128, ConnectorError> {
        check_active()?;
        if rows.end > input.len() {
            return Err(layout_error(path));
        }
        match self {
            Self::Identity => Ok(0),
            Self::Leaf(declaration) => {
                let width = match declaration.carrier() {
                    DataType::Int64 => 8,
                    DataType::FixedSizeBinary(width) => width as u128,
                    _ => return Err(layout_error(path)),
                };
                Ok((rows.len() as u128) * width + (rows.len() as u128).div_ceil(8))
            }
            Self::Struct(children) => {
                let array = input
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| layout_error(path))?;
                struct_read_bytes(children, array, rows, path, check_active)
            }
            Self::List(child) => {
                let array = input
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or_else(|| layout_error(path))?;
                let values = child_span(array.offsets(), &rows, path)?;
                Ok(((rows.len() as u128) + 1) * 4
                    + child.additional_read_bytes(array.values(), values, path, check_active)?)
            }
            Self::Map { entries, .. } => {
                let Self::Struct(children) = entries.as_ref() else {
                    return Ok(0);
                };
                let array = input
                    .as_any()
                    .downcast_ref::<MapArray>()
                    .ok_or_else(|| layout_error(path))?;
                let values = child_span(array.offsets(), &rows, path)?;
                Ok(((rows.len() as u128) + 1) * 4
                    + struct_read_bytes(children, array.entries(), values, path, check_active)?)
            }
        }
    }

    pub(crate) fn child(&self, ordinal: usize) -> &Self {
        match self {
            Self::Struct(children) => children.get(ordinal).unwrap_or(&Self::Identity),
            Self::List(child) | Self::Map { entries: child, .. } if ordinal == 0 => child,
            _ => &Self::Identity,
        }
    }

    pub(crate) fn write_layout(&self, input: &DataType) -> Result<DataType, &'static str> {
        let field = |field: &FieldRef, child: &Self| {
            let data_type = child.write_layout(field.data_type())?;
            Ok(Arc::new(field.as_ref().clone().with_data_type(data_type)))
        };
        Ok(match (self, input) {
            (Self::Identity, _) => input.clone(),
            (Self::Leaf(declaration), _) if input == &declaration.carrier() => {
                storage_layout(*declaration)
            }
            (Self::Struct(children), DataType::Struct(fields))
                if children.len() == fields.len() =>
            {
                DataType::Struct(
                    fields
                        .iter()
                        .zip(children)
                        .map(|(input, child)| field(input, child))
                        .collect::<Result<Vec<_>, &'static str>>()?
                        .into(),
                )
            }
            (Self::List(child), DataType::List(input)) => DataType::List(field(input, child)?),
            (Self::Map { entries: child, .. }, DataType::Map(input, sorted)) => {
                DataType::Map(field(input, child)?, *sorted)
            }
            _ => return Err("Iceberg temporal writer input differs from its canonical type"),
        })
    }

    pub(crate) fn materialize(
        &self,
        input: ArrayRef,
        path: &str,
        check_active: &mut impl FnMut() -> Result<(), ConnectorError>,
    ) -> Result<ArrayRef, ConnectorError> {
        self.convert(&input, path, None, check_active, read_leaf, false)
    }

    pub(crate) fn write(&self, input: &ArrayRef, path: &str) -> Result<ArrayRef, ConnectorError> {
        self.convert(input, path, None, &mut || Ok(()), write_leaf, true)
    }

    fn convert<C: FnMut() -> Result<(), ConnectorError>>(
        &self,
        input: &ArrayRef,
        path: &str,
        reachable: Option<&SelectionSet>,
        check_active: &mut C,
        leaf: ConvertTemporalLeaf<C>,
        writing: bool,
    ) -> Result<ArrayRef, ConnectorError> {
        match self {
            Self::Identity => Ok(input.clone()),
            Self::Leaf(declaration) => leaf(input, *declaration, path, reachable, check_active),
            Self::Struct(children) => {
                let array = input
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| layout_error(path))?;
                if array.num_columns() != children.len() {
                    return Err(layout_error(path));
                }
                let mut columns = Vec::with_capacity(children.len());
                let mut fields = Vec::with_capacity(children.len());
                for (ordinal, ((projection, column), field)) in children
                    .iter()
                    .zip(array.columns())
                    .zip(array.fields())
                    .enumerate()
                {
                    let column = if projection.is_identity() {
                        column.clone()
                    } else {
                        let child_path = format!("{path}.{}", field.name());
                        let rows =
                            child_rows(input, reachable, ordinal, 0..column.len(), check_active)?;
                        projection.convert(
                            column,
                            &child_path,
                            Some(&rows),
                            check_active,
                            leaf,
                            writing,
                        )?
                    };
                    fields.push(projected_field(field, &column));
                    columns.push(column);
                }
                let array = StructArray::try_new_with_length(
                    Fields::from(fields),
                    columns,
                    array.nulls().cloned(),
                    array.len(),
                )
                .map_err(|error| layout_error_detail(path, error))?;
                Ok(Arc::new(array))
            }
            Self::List(element) => {
                let DataType::List(field) = input.data_type() else {
                    return Err(layout_error(path));
                };
                let array = input
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or_else(|| layout_error(path))?;
                let child_path = format!("{path}.{}", field.name());
                let (offsets, span) =
                    trim_offsets(array.offsets(), array.values().len(), path, check_active)?;
                let rows = child_rows(input, reachable, 0, span.clone(), check_active)?;
                let original = array.values().slice(span.start, span.len());
                let values = element.convert(
                    &original,
                    &child_path,
                    Some(&rows),
                    check_active,
                    leaf,
                    writing,
                )?;
                let array = ListArray::try_new(
                    projected_field(field, &values),
                    offsets,
                    values,
                    array.nulls().cloned(),
                )
                .map_err(|error| layout_error_detail(path, error))?;
                Ok(Arc::new(array))
            }
            Self::Map {
                entries,
                check_keys,
            } => {
                let DataType::Map(field, sorted) = input.data_type() else {
                    return Err(layout_error(path));
                };
                let array = input
                    .as_any()
                    .downcast_ref::<MapArray>()
                    .ok_or_else(|| layout_error(path))?;
                let validate_keys = writing && *check_keys;
                if entries.is_identity() && !validate_keys {
                    return Ok(input.clone());
                }
                let child_path = format!("{path}.{}", field.name());
                let (offsets, span) =
                    trim_offsets(array.offsets(), array.entries().len(), path, check_active)?;
                let rows = child_rows(input, reachable, 0, span.clone(), check_active)?;
                let original: ArrayRef = Arc::new(array.entries().slice(span.start, span.len()));
                let entries = entries.convert(
                    &original,
                    &child_path,
                    Some(&rows),
                    check_active,
                    leaf,
                    writing,
                )?;
                let entries_field = projected_field(field, &entries);
                let entries = entries
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| layout_error(path))?
                    .clone();
                let array = MapArray::try_new(
                    entries_field,
                    offsets,
                    entries,
                    array.nulls().cloned(),
                    *sorted,
                )
                .map_err(|error| layout_error_detail(path, error))?;
                if validate_keys {
                    validate_map_keys(&array, path, reachable)?;
                }
                Ok(Arc::new(array))
            }
        }
    }
}

fn struct_read_bytes(
    projection: &[TemporalReadProjection],
    array: &StructArray,
    rows: Range<usize>,
    path: &str,
    check_active: &mut impl FnMut() -> Result<(), ConnectorError>,
) -> Result<u128, ConnectorError> {
    if projection.len() != array.num_columns() {
        return Err(layout_error(path));
    }
    projection
        .iter()
        .zip(array.columns())
        .try_fold(0, |total, (child, array)| {
            Ok(total + child.additional_read_bytes(array, rows.clone(), path, check_active)?)
        })
}

fn child_span(
    offsets: &OffsetBuffer<i32>,
    rows: &Range<usize>,
    path: &str,
) -> Result<Range<usize>, ConnectorError> {
    let start = usize::try_from(offsets[rows.start]).map_err(|_| layout_error(path))?;
    let end = usize::try_from(offsets[rows.end]).map_err(|_| layout_error(path))?;
    Ok(start..end)
}

/// A borrowed output prefix may retain a whole child backing buffer. New
/// temporal buffers cover the prefix's child span and use its rebased offsets.
fn trim_offsets(
    offsets: &OffsetBuffer<i32>,
    child_len: usize,
    path: &str,
    check_active: &mut impl FnMut() -> Result<(), ConnectorError>,
) -> Result<(OffsetBuffer<i32>, Range<usize>), ConnectorError> {
    let start = usize::try_from(offsets[0]).map_err(|_| layout_error(path))?;
    let end = usize::try_from(offsets.last()).map_err(|_| layout_error(path))?;
    if end < start || end > child_len {
        return Err(layout_error(path));
    }
    let span = start..end;
    if start == 0 {
        return Ok((offsets.clone(), span));
    }
    let mut rebased = Vec::new();
    rebased.try_reserve_exact(offsets.len()).map_err(|error| {
        ConnectorError::new(ConnectorErrorKind::ResourceExhausted, error.to_string())
    })?;
    for offset in offsets.chunks(256) {
        check_active()?;
        rebased.extend(offset.iter().map(|value| *value - offsets[0]));
    }
    Ok((OffsetBuffer::new(rebased.into()), span))
}

pub(crate) fn contains_temporal(ty: &Type) -> bool {
    match ty {
        Type::Primitive(primitive) => super::declaration(primitive).is_some(),
        Type::Struct(fields) => fields
            .fields()
            .iter()
            .any(|field| contains_temporal(&field.field_type)),
        Type::List(list) => contains_temporal(&list.element_field.field_type),
        Type::Map(map) => {
            contains_temporal(&map.key_field.field_type)
                || contains_temporal(&map.value_field.field_type)
        }
        Type::Variant(_) => false,
    }
}

fn validate_map_keys(
    map: &MapArray,
    path: &str,
    reachable: Option<&SelectionSet>,
) -> Result<(), ConnectorError> {
    let keys =
        normalize_key_floats(map.keys()).map_err(|error| layout_error_detail(path, error))?;
    let converter = RowConverter::new(vec![SortField::new(keys.data_type().clone())])
        .map_err(|error| layout_error_detail(path, error))?;
    let encoded = converter
        .convert_columns(&[keys])
        .map_err(|error| layout_error_detail(path, error))?;
    let offsets = map.value_offsets();
    let mut seen = HashSet::new();
    for row in 0..map.len() {
        if map.is_null(row) || reachable.is_some_and(|reachable| !reachable.contains(row)) {
            continue;
        }
        seen.clear();
        for key in offsets[row] as usize..offsets[row + 1] as usize {
            if !seen.insert(encoded.row(key)) {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    format!(
                        "Iceberg write field `{path}` has duplicate MAP keys at row {row} after temporal assignment"
                    ),
                ));
            }
        }
    }
    Ok(())
}

/// Arrow Row equality is byte equality. Match the engine's SQL float equality
/// for signed zero and NaN before using it on nested physical storage keys.
fn normalize_key_floats(array: &ArrayRef) -> Result<ArrayRef, arrow::error::ArrowError> {
    match array.data_type() {
        DataType::Float32 => {
            let array = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .expect("bound float32");
            Ok(Arc::new(Float32Array::from_iter(array.iter().map(
                |value| {
                    value.map(|value| {
                        if value == 0.0 {
                            0.0
                        } else if value.is_nan() {
                            f32::NAN
                        } else {
                            value
                        }
                    })
                },
            ))))
        }
        DataType::Float64 => {
            let array = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("bound float64");
            Ok(Arc::new(Float64Array::from_iter(array.iter().map(
                |value| {
                    value.map(|value| {
                        if value == 0.0 {
                            0.0
                        } else if value.is_nan() {
                            f64::NAN
                        } else {
                            value
                        }
                    })
                },
            ))))
        }
        DataType::Struct(_) | DataType::List(_) | DataType::Map(_, _) => {
            let data = array.to_data();
            let children = data
                .child_data()
                .iter()
                .map(|child| {
                    normalize_key_floats(&make_array(child.clone())).map(|child| child.to_data())
                })
                .collect::<Result<Vec<_>, _>>()?;
            data.into_builder()
                .child_data(children)
                .build()
                .map(make_array)
        }
        _ => Ok(array.clone()),
    }
}

fn projected_field(field: &FieldRef, column: &ArrayRef) -> FieldRef {
    if field.data_type() == column.data_type() {
        return field.clone();
    }
    Arc::new(
        field
            .as_ref()
            .clone()
            .with_data_type(column.data_type().clone()),
    )
}

fn child_rows(
    input: &ArrayRef,
    reachable: Option<&SelectionSet>,
    ordinal: usize,
    span: Range<usize>,
    check_active: &mut impl FnMut() -> Result<(), ConnectorError>,
) -> Result<SelectionSet, ConnectorError> {
    let resource = |error: novarocks_value_arrays::ValueError| {
        ConnectorError::new(ConnectorErrorKind::ResourceExhausted, error.to_string())
    };
    let mut rows = SelectionSet::try_new(span.len()).map_err(resource)?;
    let parents =
        (0..input.len()).filter(|row| reachable.is_none_or(|reachable| reachable.contains(*row)));
    visit_reachable_child_rows_observed(
        input,
        parents,
        &mut |_| check_active(),
        &mut |index, children| {
            if index == ordinal {
                for child in children {
                    let child = child
                        .checked_sub(span.start)
                        .ok_or_else(|| layout_error("nested temporal"))?;
                    rows.insert(child).map_err(resource)?;
                }
            }
            Ok(())
        },
    )
    .map_err(|error| match error {
        novarocks_value_arrays::ValueFailure::Caller(error) => error,
        novarocks_value_arrays::ValueFailure::Value(error) => {
            layout_error_detail("nested temporal", error)
        }
    })?;
    Ok(rows)
}

fn read_leaf(
    input: &ArrayRef,
    declaration: TemporalType,
    path: &str,
    reachable: Option<&SelectionSet>,
    check_active: &mut impl FnMut() -> Result<(), ConnectorError>,
) -> Result<ArrayRef, ConnectorError> {
    if !super::matches_read_storage_layout(declaration, input.data_type()) {
        return Err(layout_error(path));
    }
    macro_rules! read_storage {
        ($array:ty) => {{
            let array = input
                .as_any()
                .downcast_ref::<$array>()
                .ok_or_else(|| layout_error(path))?;
            &array.values()[..]
        }};
    }
    let storage = match declaration {
        TemporalType::Time(_) => read_storage!(Time64MicrosecondArray),
        TemporalType::Timestamp(precision) | TemporalType::TimestampWithTimeZone(precision) => {
            if precision.fits_microseconds() {
                read_storage!(TimestampMicrosecondArray)
            } else {
                read_storage!(TimestampNanosecondArray)
            }
        }
        _ => return Err(layout_error(path)),
    };
    let mut value_at =
        |row| -> Result<Option<TemporalCarrier>, ConnectorError> {
            if row % 256 == 0 {
                check_active()?;
            }
            if input.is_null(row) {
                return Ok(None);
            }
            let storage = if reachable.is_some_and(|rows| !rows.contains(row)) {
                0
            } else {
                storage[row]
            };
            let value = super::from_storage(declaration, storage)
            .and_then(|value| value.to_carrier(declaration))
            .map_err(|error| ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                format!("Iceberg temporal field `{path}` is outside its SQL value range: {error}"),
            ))?;
            Ok(Some(value))
        };

    match declaration.carrier() {
        DataType::Int64 => {
            let mut builder = arrow::array::Int64Builder::with_capacity(input.len());
            for row in 0..input.len() {
                match value_at(row)? {
                    Some(TemporalCarrier::I64(value)) => builder.append_value(value),
                    None => builder.append_null(),
                    _ => return Err(layout_error(path)),
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::FixedSizeBinary(width) => {
            let mut builder = FixedSizeBinaryBuilder::with_capacity(input.len(), width);
            for row in 0..input.len() {
                match value_at(row)? {
                    Some(TemporalCarrier::Wide(value)) => builder
                        .append_value(value)
                        .map_err(|error| layout_error_detail(path, error))?,
                    Some(TemporalCarrier::ZonedTimestamp(value)) => builder
                        .append_value(value)
                        .map_err(|error| layout_error_detail(path, error))?,
                    None => builder.append_null(),
                    _ => return Err(layout_error(path)),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        _ => Err(layout_error(path)),
    }
}

fn storage_layout(declaration: TemporalType) -> DataType {
    match declaration {
        TemporalType::Time(_) => DataType::Time64(TimeUnit::Microsecond),
        TemporalType::Timestamp(precision) | TemporalType::TimestampWithTimeZone(precision) => {
            let unit = if precision.fits_microseconds() {
                TimeUnit::Microsecond
            } else {
                TimeUnit::Nanosecond
            };
            let zone = matches!(declaration, TemporalType::TimestampWithTimeZone(_))
                .then(|| "+00:00".into());
            DataType::Timestamp(unit, zone)
        }
        _ => unreachable!("only an Iceberg temporal primitive creates a storage projection"),
    }
}

fn write_leaf(
    input: &ArrayRef,
    declaration: TemporalType,
    path: &str,
    reachable: Option<&SelectionSet>,
    check_active: &mut impl FnMut() -> Result<(), ConnectorError>,
) -> Result<ArrayRef, ConnectorError> {
    let prepared = PreparedTemporalArray::bind(declaration, input.as_ref(), true)
        .map_err(|error| layout_error_detail(path, error))?;
    let values = (0..input.len())
        .map(|row| {
            if row % 256 == 0 {
                check_active()?;
            }
            if input.is_null(row) {
                return Ok(None);
            }
            if reachable.is_some_and(|reachable| !reachable.contains(row)) {
                return Ok(Some(0));
            }
            let invalid = |error: &dyn std::fmt::Display| {
                ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    format!("Iceberg temporal field `{path}` cannot be written: {error}"),
                )
            };
            let value = prepared.value(row).map_err(|error| invalid(&error))?;
            value
                .map(|value| super::to_storage(declaration, value).map_err(|error| invalid(&error)))
                .transpose()
        })
        .collect::<Result<Vec<_>, ConnectorError>>()?;
    Ok(match storage_layout(declaration) {
        DataType::Time64(TimeUnit::Microsecond) => {
            Arc::new(Time64MicrosecondArray::from(values)) as ArrayRef
        }
        DataType::Timestamp(TimeUnit::Microsecond, zone) => {
            Arc::new(TimestampMicrosecondArray::from(values).with_timezone_opt(zone)) as ArrayRef
        }
        DataType::Timestamp(TimeUnit::Nanosecond, zone) => {
            Arc::new(TimestampNanosecondArray::from(values).with_timezone_opt(zone)) as ArrayRef
        }
        _ => unreachable!("only an Iceberg temporal primitive creates a storage projection"),
    })
}

fn layout_error(path: &str) -> ConnectorError {
    layout_error_detail(path, "the bound physical array has a different shape")
}

fn layout_error_detail(path: &str, detail: impl std::fmt::Display) -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::CorruptData,
        format!("Iceberg temporal field `{path}` cannot be projected: {detail}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iceberg::spec::{ListType, MapType, NestedField, PrimitiveType};
    use arrow::{array::Int32Array, buffer::OffsetBuffer, datatypes::Field};
    use novarocks_type_contract::{TemporalMicros, TemporalValue, TimeZoneKey};

    #[test]
    fn temporal_projection_reads_compact_aliases_and_rejects_unrepresentable_raw_zoned_values() {
        let compact = TemporalReadProjection::from_type(&Type::Primitive(PrimitiveType::Timestamp));
        let array: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![Some(-1), None]));
        let projected = compact
            .materialize(array.clone(), "compact", &mut || Ok(()))
            .unwrap();
        assert!(Arc::ptr_eq(&array, &projected));

        let zoned = TemporalReadProjection::from_type(&Type::Primitive(PrimitiveType::Timestamptz));
        let array: ArrayRef =
            Arc::new(TimestampMicrosecondArray::from(vec![Some(-1), None]).with_timezone_utc());
        let projected = zoned.materialize(array, "zoned", &mut || Ok(())).unwrap();
        let declaration = super::super::declaration(&PrimitiveType::Timestamptz).unwrap();
        let values = PreparedTemporalArray::bind(declaration, projected.as_ref(), true).unwrap();
        assert_eq!(
            values.value(0).unwrap(),
            Some(TemporalValue::TimestampWithTimeZone {
                instant: TemporalMicros::whole_micros(-1),
                zone: TimeZoneKey::UTC,
            })
        );
        assert_eq!(values.value(1).unwrap(), None);
        let too_wide: ArrayRef = Arc::new(
            TimestampMicrosecondArray::from(vec![(1_i64 << 51) * 1_000]).with_timezone_utc(),
        );
        assert_eq!(
            zoned
                .materialize(too_wide, "zoned", &mut || Ok(()))
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::InvalidRequest,
        );
    }

    #[test]
    fn temporal_projection_rejects_map_keys_collapsed_to_the_same_storage_value() {
        let map_type = Type::Map(MapType::new(
            NestedField::map_key_element(2, Type::Primitive(PrimitiveType::Timestamp)).into(),
            NestedField::map_value_element(3, Type::Primitive(PrimitiveType::Int), false).into(),
        ));
        let fields = Fields::from(vec![
            Arc::new(Field::new(
                "key",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            )),
            Arc::new(Field::new("value", DataType::Int32, true)),
        ]);
        let entries = StructArray::new(
            fields.clone(),
            vec![
                Arc::new(TimestampMicrosecondArray::from(vec![0, 0])),
                Arc::new(Int32Array::from(vec![1, 2])),
            ],
            None,
        );
        let array: ArrayRef = Arc::new(MapArray::new(
            Arc::new(Field::new("entries", DataType::Struct(fields), false)),
            OffsetBuffer::new(vec![0, 2].into()),
            entries,
            None,
            false,
        ));
        let error = TemporalReadProjection::from_type(&map_type)
            .write(&array, "payload")
            .unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert!(error.message().contains("duplicate MAP keys"));
    }

    #[test]
    fn temporal_projection_only_copies_children_in_the_output_prefix() {
        let list_type = Type::List(ListType::new(
            NestedField::list_element(2, Type::Primitive(PrimitiveType::Timestamptz), false).into(),
        ));
        let values: ArrayRef = Arc::new(
            TimestampMicrosecondArray::from(vec![i64::MAX, -1, 0, i64::MAX]).with_timezone_utc(),
        );
        let source = ListArray::new(
            Arc::new(Field::new("item", values.data_type().clone(), true)),
            OffsetBuffer::new(vec![0, 1, 3, 4].into()),
            values,
            None,
        );
        let selected: ArrayRef = Arc::new(source.slice(1, 1));
        let output = TemporalReadProjection::from_type(&list_type)
            .materialize(selected, "clock", &mut || Ok(()))
            .unwrap();
        let list = output.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(list.value_offsets(), &[0, 2]);
        assert_eq!(list.values().len(), 2);
        let ty = super::super::declaration(&PrimitiveType::Timestamptz).unwrap();
        let result = PreparedTemporalArray::bind(ty, list.values().as_ref(), true).unwrap();
        assert_eq!(
            result.value(0).unwrap(),
            Some(super::super::from_storage(ty, -1).unwrap())
        );
    }
}
