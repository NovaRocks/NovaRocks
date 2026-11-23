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

//! Lossless execution-carrier adaptation to an authoritative Iceberg SDK schema.

use std::{fmt, sync::Arc};

use crate::temporal::TemporalReadProjection;
use arrow::array::{
    Array, ArrayRef, BinaryArray, DictionaryArray, FixedSizeBinaryArray, FixedSizeBinaryBuilder,
    Int8Array, Int16Array, Int32Array, LargeBinaryArray, LargeStringArray, ListArray, MapArray,
    StringBuilder, StructArray, make_array,
};
use arrow::datatypes::{DataType, Field, Int32Type, SchemaRef};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
use novarocks_value_arrays::{reachability::SelectionSet, visit_reachable_child_rows_observed};

/// Value failures remain distinct from broken writer contracts until the
/// production append boundary assigns the connector failure kind.
#[derive(Debug)]
pub(crate) enum WriteAdaptationError {
    ValueWidth {
        path: String,
        expected: usize,
        actual: usize,
    },
    RequiredNull {
        path: String,
    },
    Contract(String),
    Temporal(ConnectorError),
}
impl fmt::Display for WriteAdaptationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ValueWidth {
                path,
                expected,
                actual,
            } => write!(
                f,
                "Iceberg write adaptation fixed-width value at `{path}` requires {expected} bytes, got {actual}"
            ),
            Self::RequiredNull { path } => write!(
                f,
                "Iceberg write adaptation required column `{path}` contains NULL; the upstream writer obligation must reject it before Provider I/O"
            ),
            Self::Contract(message) => message.fmt(f),
            Self::Temporal(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for WriteAdaptationError {}
impl From<String> for WriteAdaptationError {
    fn from(message: String) -> Self {
        Self::Contract(message)
    }
}
impl WriteAdaptationError {
    pub(crate) fn into_connector_error(self) -> ConnectorError {
        if let Self::Temporal(error) = self {
            return error;
        }
        let kind = match self {
            Self::ValueWidth { .. } => ConnectorErrorKind::InvalidRequest,
            Self::RequiredNull { .. } | Self::Contract(_) => ConnectorErrorKind::Internal,
            Self::Temporal(_) => unreachable!("temporal failure is already typed"),
        };
        ConnectorError::new(kind, self.to_string())
    }
}

/// The target has already been projected into input order by the frozen-schema
/// owner. This boundary does not resolve fields, fill columns, or cast values.
pub(super) fn adapt_batch(
    input: &RecordBatch,
    target: &SchemaRef,
) -> Result<RecordBatch, WriteAdaptationError> {
    validate_batch_schema(input.schema().as_ref(), target.as_ref())?;
    let source = input.schema();
    let columns = input
        .columns()
        .iter()
        .zip(source.fields())
        .zip(target.fields())
        .map(|((array, from), to)| adapt_array(array, from, to, to.name(), true, None))
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new_with_options(
        target.clone(),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(input.num_rows())),
    )
    .map_err(|error| {
        WriteAdaptationError::Contract(format!("Iceberg write adaptation batch failed: {error}"))
    })
}

pub(super) fn validate_temporal_batch_schema(
    input: &arrow::datatypes::Schema,
    target: &arrow::datatypes::Schema,
    temporals: &[TemporalReadProjection],
) -> Result<(), String> {
    if temporals.len() != input.fields().len() {
        return Err("Iceberg write storage projection has a different width from its input".into());
    }
    if temporals.iter().all(TemporalReadProjection::is_identity) {
        return validate_batch_schema(input, target);
    }
    let fields = input
        .fields()
        .iter()
        .zip(temporals)
        .map(|(field, projection)| {
            let data_type = projection.write_layout(field.data_type())?;
            Ok(field.as_ref().clone().with_data_type(data_type))
        })
        .collect::<Result<Vec<_>, &'static str>>()?;
    validate_batch_schema(&arrow::datatypes::Schema::new(fields), target)
}

pub(super) fn adapt_temporal_batch(
    input: &RecordBatch,
    target: &SchemaRef,
    temporals: &[TemporalReadProjection],
) -> Result<RecordBatch, WriteAdaptationError> {
    validate_temporal_batch_schema(input.schema().as_ref(), target, temporals)?;
    if temporals.iter().all(TemporalReadProjection::is_identity) {
        return adapt_batch(input, target);
    }
    let input_schema = input.schema();
    let mut fields = Vec::with_capacity(input.num_columns());
    let mut columns = Vec::with_capacity(input.num_columns());
    for ((array, field), projection) in input
        .columns()
        .iter()
        .zip(input_schema.fields())
        .zip(temporals)
    {
        let array = projection
            .write(array, field.name())
            .map_err(WriteAdaptationError::Temporal)?;
        fields.push(
            field
                .as_ref()
                .clone()
                .with_data_type(array.data_type().clone()),
        );
        columns.push(array);
    }
    let batch = RecordBatch::try_new_with_options(
        Arc::new(arrow::datatypes::Schema::new(fields)),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(input.num_rows())),
    )
    .map_err(|error| WriteAdaptationError::Contract(error.to_string()))?;
    adapt_batch(&batch, target)
}

fn nested_nullability(from: &Field, to: &Field, path: &str) -> Result<(), String> {
    if from.is_nullable() != to.is_nullable() {
        return Err(format!(
            "Iceberg write adaptation nested nullability mismatch at `{path}`: input={} target={}",
            from.is_nullable(),
            to.is_nullable()
        ));
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum Adaptation {
    Identity,
    Int8,
    Int16,
    Binary,
    FixedWidth(i32),
    Timezone,
    Dictionary,
    Struct,
    List,
    Map,
}

fn adaptation(
    from: &DataType,
    to: &DataType,
    path: &str,
    top_level: bool,
) -> Result<Adaptation, String> {
    Ok(match (from, to) {
        (x, y) if x == y => Adaptation::Identity,
        (DataType::Int8, DataType::Int32) if top_level => Adaptation::Int8,
        (DataType::Int16, DataType::Int32) if top_level => Adaptation::Int16,
        (DataType::Binary, DataType::LargeBinary) => Adaptation::Binary,
        (DataType::Binary | DataType::FixedSizeBinary(_), DataType::FixedSizeBinary(width)) => {
            if *width < 0 {
                return Err(format!(
                    "Iceberg write adaptation invalid fixed width at `{path}`: {width}"
                ));
            }
            Adaptation::FixedWidth(*width)
        }
        (DataType::Timestamp(u, None), DataType::Timestamp(v, Some(tz)))
            if u == v && tz.as_ref() == "+00:00" =>
        {
            Adaptation::Timezone
        }
        (DataType::Dictionary(k, v), DataType::Utf8)
            if **k == DataType::Int32
                && matches!(v.as_ref(), DataType::Utf8 | DataType::LargeUtf8) =>
        {
            Adaptation::Dictionary
        }
        (DataType::Struct(_), DataType::Struct(_)) => Adaptation::Struct,
        (DataType::List(_), DataType::List(_)) => Adaptation::List,
        (DataType::Map(_, s1), DataType::Map(_, s2)) if s1 == s2 => Adaptation::Map,
        _ => {
            return Err(format!(
                "unsupported Iceberg write adaptation at `{path}`: {from:?} to {to:?}"
            ));
        }
    })
}

/// Pure preparation shares the exact conversion vocabulary with batch writing.
/// Required actual NULLs and string offset bounds need arrays and remain checked
/// by `adapt_batch` before publishing bytes.
pub(crate) fn validate_batch_schema(
    input: &arrow::datatypes::Schema,
    target: &arrow::datatypes::Schema,
) -> Result<(), String> {
    if input.fields().len() != target.fields().len() {
        return Err(format!(
            "Iceberg write adaptation column count mismatch: input={} target={}",
            input.fields().len(),
            target.fields().len()
        ));
    }
    for (from, to) in input.fields().iter().zip(target.fields()) {
        if !from.name().eq_ignore_ascii_case(to.name()) {
            return Err(format!(
                "Iceberg write adaptation field name mismatch: input `{}` target `{}`",
                from.name(),
                to.name()
            ));
        }
        validate_field_schema(from, to, to.name(), true, false)?;
    }
    Ok(())
}

fn validate_field_schema(
    from: &Field,
    to: &Field,
    path: &str,
    top_level: bool,
    canonical_struct_roles: bool,
) -> Result<(), String> {
    if !top_level {
        nested_nullability(from, to, path)?;
    }
    match adaptation(from.data_type(), to.data_type(), path, top_level)? {
        Adaptation::Struct => {
            let (DataType::Struct(sources), DataType::Struct(targets)) =
                (from.data_type(), to.data_type())
            else {
                unreachable!()
            };
            if sources.len() != targets.len() {
                return Err(format!(
                    "Iceberg write adaptation struct child count mismatch at `{path}`"
                ));
            }
            for (source, target) in sources.iter().zip(targets) {
                if !canonical_struct_roles && !source.name().eq_ignore_ascii_case(target.name()) {
                    return Err(format!(
                        "Iceberg write adaptation struct field name mismatch at `{path}.{}`: input `{}` target `{}`",
                        target.name(),
                        source.name(),
                        target.name()
                    ));
                }
                validate_field_schema(
                    source,
                    target,
                    &format!("{path}.{}", target.name()),
                    false,
                    false,
                )?;
            }
        }
        Adaptation::List | Adaptation::Map => {
            let (source, target, canonical_struct_roles) = match (from.data_type(), to.data_type())
            {
                (DataType::List(s), DataType::List(t)) => (s, t, false),
                (DataType::Map(s, _), DataType::Map(t, _)) => (s, t, true),
                _ => unreachable!(),
            };
            validate_field_schema(
                source,
                target,
                &format!("{path}.{}", target.name()),
                false,
                canonical_struct_roles,
            )?;
        }
        _ => (),
    }
    Ok(())
}

/// Schemas without fixed values or nested required fields retain the existing
/// direct path. A top-level requirement is checked without a child selection.
fn needs_child_rows(data_type: &DataType) -> bool {
    match data_type {
        DataType::FixedSizeBinary(_) => true,
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| !field.is_nullable() || needs_child_rows(field.data_type())),
        DataType::List(field) | DataType::Map(field, _) => {
            !field.is_nullable() || needs_child_rows(field.data_type())
        }
        _ => false,
    }
}

fn child_reachable_rows(
    array: &ArrayRef,
    reachable: Option<&SelectionSet>,
    ordinal: usize,
    child_len: usize,
) -> Result<SelectionSet, WriteAdaptationError> {
    let mut selected = SelectionSet::try_new(child_len).map_err(|error| error.to_string())?;
    // Only value-arrays authors parent validity and actual child offsets.
    let parent_rows =
        (0..array.len()).filter(|row| reachable.is_none_or(|rows| rows.contains(*row)));
    visit_reachable_child_rows_observed(
        array,
        parent_rows,
        &mut |_| Ok::<_, String>(()),
        &mut |index, rows| {
            if index == ordinal {
                for row in rows {
                    selected.insert(row).map_err(|error| error.to_string())?;
                }
            }
            Ok(())
        },
    )
    .map_err(|error| error.to_string())?;
    Ok(selected)
}

fn check_required_values(
    array: &ArrayRef,
    to: &Field,
    path: &str,
    reachable: Option<&SelectionSet>,
) -> Result<(), WriteAdaptationError> {
    if !to.is_nullable()
        && array.null_count() != 0
        && (0..array.len())
            .any(|row| reachable.is_none_or(|rows| rows.contains(row)) && array.is_null(row))
    {
        return Err(WriteAdaptationError::RequiredNull {
            path: path.to_owned(),
        });
    }
    Ok(())
}

fn adapt_fixed_width(
    array: &ArrayRef,
    to: &Field,
    path: &str,
    width: i32,
    reachable: Option<&SelectionSet>,
) -> Result<ArrayRef, WriteAdaptationError> {
    let expected = usize::try_from(width)
        .map_err(|_| format!("Iceberg write adaptation invalid fixed width at `{path}`"))?;
    let mut out = FixedSizeBinaryBuilder::with_capacity(array.len(), width);
    let placeholder = vec![0u8; expected];
    for row in 0..array.len() {
        if array.is_null(row) || reachable.is_some_and(|rows| !rows.contains(row)) {
            // Inactive required slots must remain constructible even when a
            // list's full values array contains unreferenced physical NULLs.
            if array.is_null(row) && to.is_nullable() {
                out.append_null();
            } else {
                out.append_value(&placeholder)
                    .map_err(|error| error.to_string())?;
            }
            continue;
        }
        let bytes = match array.data_type() {
            DataType::Binary => array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("Binary array")
                .value(row),
            DataType::FixedSizeBinary(_) => array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .expect("FixedSizeBinary array")
                .value(row),
            _ => unreachable!("prepared fixed-width carrier"),
        };
        if bytes.len() != expected {
            return Err(WriteAdaptationError::ValueWidth {
                path: path.to_owned(),
                expected,
                actual: bytes.len(),
            });
        }
        out.append_value(bytes).map_err(|error| error.to_string())?;
    }
    Ok(Arc::new(out.finish()))
}

fn adapt_array(
    array: &ArrayRef,
    from: &Field,
    to: &Field,
    path: &str,
    top_level: bool,
    reachable: Option<&SelectionSet>,
) -> Result<ArrayRef, WriteAdaptationError> {
    if !top_level {
        nested_nullability(from, to, path)?;
    }
    check_required_values(array, to, path, reachable)?;
    let target = to.data_type();
    let kind = match (
        adaptation(array.data_type(), target, path, top_level)?,
        target,
    ) {
        (Adaptation::Identity, DataType::Struct(_)) if needs_child_rows(target) => {
            Adaptation::Struct
        }
        (Adaptation::Identity, DataType::List(_)) if needs_child_rows(target) => Adaptation::List,
        (Adaptation::Identity, DataType::Map(_, _)) if needs_child_rows(target) => Adaptation::Map,
        (kind, _) => kind,
    };
    let output = match kind {
        Adaptation::Identity => array.clone(),
        Adaptation::Int8 => {
            let a = array
                .as_any()
                .downcast_ref::<Int8Array>()
                .expect("Int8 array");
            Arc::new(Int32Array::from_iter(a.iter().map(|v| v.map(i32::from)))) as ArrayRef
        }
        Adaptation::Int16 => {
            let a = array
                .as_any()
                .downcast_ref::<Int16Array>()
                .expect("Int16 array");
            Arc::new(Int32Array::from_iter(a.iter().map(|v| v.map(i32::from)))) as ArrayRef
        }
        Adaptation::Binary => {
            let a = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("Binary array");
            Arc::new(LargeBinaryArray::new(
                arrow::buffer::OffsetBuffer::new(
                    a.value_offsets()
                        .iter()
                        .map(|v| i64::from(*v))
                        .collect::<Vec<_>>()
                        .into(),
                ),
                a.values().clone(),
                a.nulls().cloned(),
            )) as ArrayRef
        }
        Adaptation::FixedWidth(width) => adapt_fixed_width(array, to, path, width, reachable)?,
        Adaptation::Timezone => {
            let data = array
                .to_data()
                .into_builder()
                .data_type(target.clone())
                .build()
                .map_err(|error| {
                    format!("Iceberg write adaptation timezone label at `{path}` failed: {error}")
                })?;
            make_array(data)
        }
        Adaptation::Dictionary => {
            let dictionary = array
                .as_any()
                .downcast_ref::<DictionaryArray<Int32Type>>()
                .expect("Int32 dictionary");
            let unpacked =
                arrow::compute::take(dictionary.values().as_ref(), dictionary.keys(), None)
                    .map_err(|error| {
                        format!("Iceberg write adaptation dictionary at `{path}` failed: {error}")
                    })?;
            let unpacked = if matches!(unpacked.data_type(), DataType::LargeUtf8)
                && *target == DataType::Utf8
            {
                checked_string_offsets(&unpacked, path)?
            } else {
                unpacked
            };
            let unpacked_field = from.clone().with_data_type(unpacked.data_type().clone());
            adapt_array(&unpacked, &unpacked_field, to, path, top_level, reachable)?
        }
        Adaptation::Struct => {
            let (DataType::Struct(source_fields), DataType::Struct(target_fields)) =
                (array.data_type(), target)
            else {
                unreachable!()
            };
            let a = array
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("Struct array");
            if source_fields.len() != target_fields.len() {
                return Err(format!(
                    "Iceberg write adaptation struct child count mismatch at `{path}`"
                )
                .into());
            }
            let rows = if needs_child_rows(target) && !target_fields.is_empty() {
                Some(child_reachable_rows(array, reachable, 0, a.len())?)
            } else {
                None
            };
            let columns = a
                .columns()
                .iter()
                .zip(source_fields)
                .zip(target_fields)
                .map(|((child, source), target)| {
                    adapt_array(
                        child,
                        source,
                        target,
                        &format!("{path}.{}", target.name()),
                        false,
                        rows.as_ref(),
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            if array.data_type() == target
                && columns
                    .iter()
                    .zip(a.columns())
                    .all(|(x, y)| Arc::ptr_eq(x, y))
            {
                array.clone()
            } else {
                Arc::new(
                    StructArray::try_new_with_length(
                        target_fields.clone(),
                        columns,
                        a.nulls().cloned(),
                        a.len(),
                    )
                    .map_err(|error| {
                        format!("Iceberg write adaptation struct at `{path}` failed: {error}")
                    })?,
                ) as ArrayRef
            }
        }
        Adaptation::List => {
            let (DataType::List(source), DataType::List(target)) = (array.data_type(), target)
            else {
                unreachable!()
            };
            let a = array
                .as_any()
                .downcast_ref::<ListArray>()
                .expect("List array");
            let rows = if !target.is_nullable() || needs_child_rows(target.data_type()) {
                Some(child_reachable_rows(array, reachable, 0, a.values().len())?)
            } else {
                None
            };
            let values = adapt_array(
                a.values(),
                source,
                target,
                &format!("{path}.{}", target.name()),
                false,
                rows.as_ref(),
            )?;
            if source == target && Arc::ptr_eq(&values, a.values()) {
                array.clone()
            } else {
                Arc::new(
                    ListArray::try_new(
                        target.clone(),
                        a.offsets().clone(),
                        values,
                        a.nulls().cloned(),
                    )
                    .map_err(|error| {
                        format!("Iceberg write adaptation list at `{path}` failed: {error}")
                    })?,
                ) as ArrayRef
            }
        }
        Adaptation::Map => {
            let (DataType::Map(source, _), DataType::Map(target, target_sorted)) =
                (array.data_type(), target)
            else {
                unreachable!()
            };
            let a = array
                .as_any()
                .downcast_ref::<MapArray>()
                .expect("Map array");
            let rows = if needs_child_rows(target.data_type()) {
                Some(child_reachable_rows(
                    array,
                    reachable,
                    0,
                    a.entries().len(),
                )?)
            } else {
                None
            };
            let original_entries = Arc::new(a.entries().clone()) as ArrayRef;
            let entries = adapt_array(
                &original_entries,
                source,
                target,
                &format!("{path}.{}", target.name()),
                false,
                rows.as_ref(),
            )?;
            if source == target && Arc::ptr_eq(&entries, &original_entries) {
                array.clone()
            } else {
                let entries = entries
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .expect("adapted map entries");
                Arc::new(
                    MapArray::try_new(
                        target.clone(),
                        a.offsets().clone(),
                        entries.clone(),
                        a.nulls().cloned(),
                        *target_sorted,
                    )
                    .map_err(|error| {
                        format!("Iceberg write adaptation map at `{path}` failed: {error}")
                    })?,
                ) as ArrayRef
            }
        }
    };
    check_required_values(&output, to, path, reachable)?;
    Ok(output)
}

fn checked_utf8_total(total: usize, additional: usize, path: &str) -> Result<usize, String> {
    total
        .checked_add(additional)
        .filter(|total| *total <= i32::MAX as usize)
        .ok_or_else(|| {
            format!("Iceberg write adaptation string offsets exceed i32 range at `{path}`")
        })
}

fn checked_string_offsets(array: &ArrayRef, path: &str) -> Result<ArrayRef, String> {
    let a = array
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .expect("LargeUtf8 array");
    let total = a.iter().flatten().try_fold(0, |total, value| {
        checked_utf8_total(total, value.len(), path)
    })?;
    let mut builder = StringBuilder::with_capacity(a.len(), total);
    for value in a.iter() {
        builder.append_option(value);
    }
    Ok(Arc::new(builder.finish()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Decimal128Array, Int64Array, LargeStringArray, StringArray, TimestampMicrosecondArray,
        TimestampNanosecondArray,
    };
    use arrow::buffer::{NullBuffer, OffsetBuffer};
    use arrow::datatypes::{Fields, Schema, TimeUnit};
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

    fn field(name: &str, dtype: DataType, nullable: bool, id: i32) -> Field {
        Field::new(name, dtype, nullable).with_metadata(std::collections::HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            id.to_string(),
        )]))
    }

    fn adapt_one(array: ArrayRef, target: Field) -> Result<RecordBatch, WriteAdaptationError> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            target.name(),
            array.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
        adapt_batch(&batch, &Arc::new(Schema::new(vec![target])))
    }

    fn dictionary(large: bool, nullable: bool) -> ArrayRef {
        let values: ArrayRef = if large {
            Arc::new(LargeStringArray::from(vec![
                Some("alpha"),
                None,
                Some("東京"),
            ]))
        } else {
            Arc::new(StringArray::from(vec![Some("alpha"), None, Some("東京")]))
        };
        let keys = if nullable {
            Int32Array::from(vec![Some(2), None, Some(0), Some(1)])
        } else {
            Int32Array::from(vec![2, 0, 2, 0])
        };
        Arc::new(DictionaryArray::try_new(keys, values).unwrap())
    }

    #[test]
    fn top_level_narrow_integers_widen_without_losing_values_or_nulls() {
        for array in [
            Arc::new(Int8Array::from(vec![Some(i8::MIN), None, Some(i8::MAX)])) as ArrayRef,
            Arc::new(Int16Array::from(vec![Some(i16::MIN), None, Some(i16::MAX)])) as ArrayRef,
        ] {
            let expected = if *array.data_type() == DataType::Int8 {
                vec![Some(-128), None, Some(127)]
            } else {
                vec![Some(-32768), None, Some(32767)]
            };
            let batch = adapt_one(array, field("n", DataType::Int32, true, 1)).unwrap();
            let output = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!(output.iter().collect::<Vec<_>>(), expected);
            assert_eq!(
                batch
                    .schema()
                    .field(0)
                    .metadata()
                    .get(PARQUET_FIELD_ID_META_KEY),
                Some(&"1".to_string())
            );
        }
    }

    #[test]
    fn timezone_relabel_preserves_micros_nanos_nulls_and_slices() {
        for array in [
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(-1),
                None,
                Some(42),
            ])) as ArrayRef,
            Arc::new(TimestampNanosecondArray::from(vec![
                Some(-1),
                None,
                Some(42),
            ])) as ArrayRef,
        ] {
            let unit = match array.data_type() {
                DataType::Timestamp(unit, _) => *unit,
                _ => unreachable!(),
            };
            let input = array.slice(1, 2);
            let output = adapt_one(
                input.clone(),
                field(
                    "ts",
                    DataType::Timestamp(unit, Some("+00:00".into())),
                    true,
                    9,
                ),
            )
            .unwrap();
            assert_eq!(
                input.to_data().buffers(),
                output.column(0).to_data().buffers()
            );
            assert!(output.column(0).is_null(0));
            assert_eq!(input.to_data().nulls(), output.column(0).to_data().nulls());
        }
    }

    #[test]
    fn admitted_dictionaries_expand_at_root_and_keep_logical_nulls() {
        for large in [false, true] {
            let output =
                adapt_one(dictionary(large, true), field("s", DataType::Utf8, true, 2)).unwrap();
            let strings = output
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(
                strings.iter().collect::<Vec<_>>(),
                vec![Some("東京"), None, Some("alpha"), None]
            );
        }
    }

    #[test]
    fn binary_widens_in_sliced_list_and_map_with_authoritative_nested_fields() {
        let binary: ArrayRef = Arc::new(BinaryArray::from(vec![
            Some(&[0xff, 0][..]),
            None,
            Some(&[][..]),
            Some(&[1][..]),
        ]));
        let source_child = Arc::new(Field::new("item", DataType::Binary, true));
        let list: ArrayRef = Arc::new(ListArray::new(
            source_child,
            OffsetBuffer::new(vec![0, 1, 3, 4].into()),
            binary.clone(),
            Some(NullBuffer::from(vec![true, false, true])),
        ));
        let target_child = Arc::new(field("element", DataType::LargeBinary, true, 11));
        let output = adapt_one(
            list.slice(1, 2),
            field("l", DataType::List(target_child.clone()), true, 10),
        )
        .unwrap();
        let list = output
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(list.value_offsets(), &[1, 3, 4]);
        assert!(list.is_null(0));
        assert_eq!(list.data_type(), &DataType::List(target_child));
        let values = list
            .values()
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        assert_eq!(values.value(0), &[0xff, 0]);
        assert!(values.is_null(1));
        assert_eq!(values.value(2), &[] as &[u8]);

        let sources: Fields = vec![
            Arc::new(Field::new("key", DataType::Int32, false)),
            Arc::new(Field::new("value", DataType::Binary, true)),
        ]
        .into();
        let entries = StructArray::new(
            sources.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4])), binary],
            None,
        );
        let map: ArrayRef = Arc::new(MapArray::new(
            Arc::new(Field::new("entries", DataType::Struct(sources), false)),
            OffsetBuffer::new(vec![0, 2, 4].into()),
            entries,
            None,
            false,
        ));
        let targets: Fields = vec![
            Arc::new(field("key", DataType::Int32, false, 21)),
            Arc::new(field("value", DataType::LargeBinary, true, 22)),
        ]
        .into();
        let output = adapt_one(
            map,
            field(
                "m",
                DataType::Map(
                    Arc::new(Field::new(
                        "key_value",
                        DataType::Struct(targets.clone()),
                        false,
                    )),
                    false,
                ),
                true,
                20,
            ),
        )
        .unwrap();
        let map = output
            .column(0)
            .as_any()
            .downcast_ref::<MapArray>()
            .unwrap();
        assert_eq!(map.entries().fields(), &targets);
        assert_eq!(map.value_offsets(), &[0, 2, 4]);
        assert_eq!(
            map.values()
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap()
                .value(0),
            &[0xff, 0]
        );
    }

    #[test]
    fn dictionaries_expand_recursively_in_struct_list_and_map_keys() {
        for large in [false, true] {
            let dict = dictionary(large, true);
            let source_fields: Fields =
                vec![Arc::new(Field::new("s", dict.data_type().clone(), true))].into();
            let structure: ArrayRef = Arc::new(StructArray::new(
                source_fields,
                vec![dict.clone()],
                Some(NullBuffer::from(vec![true, false, true, true])),
            ));
            let target_fields: Fields = vec![Arc::new(field("s", DataType::Utf8, true, 31))].into();
            let output = adapt_one(
                structure,
                field("obj", DataType::Struct(target_fields), true, 30),
            )
            .unwrap();
            let structure = output
                .column(0)
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap();
            assert!(structure.is_null(1));
            assert_eq!(
                structure
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0),
                "東京"
            );
            let list: ArrayRef = Arc::new(ListArray::new(
                Arc::new(Field::new("item", dict.data_type().clone(), true)),
                OffsetBuffer::new(vec![0, 2, 4].into()),
                dict,
                None,
            ));
            let output = adapt_one(
                list,
                field(
                    "l",
                    DataType::List(Arc::new(field("element", DataType::Utf8, true, 41))),
                    true,
                    40,
                ),
            )
            .unwrap();
            let list = output
                .column(0)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            assert!(list.values().is_null(3));

            let keys = dictionary(large, false);
            let fields: Fields = vec![
                Arc::new(Field::new("key", keys.data_type().clone(), false)),
                Arc::new(Field::new("value", DataType::Int32, true)),
            ]
            .into();
            let entries = StructArray::new(
                fields.clone(),
                vec![
                    keys,
                    Arc::new(Int32Array::from(vec![Some(1), None, Some(3), Some(4)])),
                ],
                None,
            );
            let map: ArrayRef = Arc::new(MapArray::new(
                Arc::new(Field::new("entries", DataType::Struct(fields), false)),
                OffsetBuffer::new(vec![0, 4].into()),
                entries,
                None,
                false,
            ));
            let fields: Fields = vec![
                Arc::new(field("key", DataType::Utf8, false, 51)),
                Arc::new(field("value", DataType::Int32, true, 52)),
            ]
            .into();
            let output = adapt_one(
                map,
                field(
                    "m",
                    DataType::Map(
                        Arc::new(Field::new("key_value", DataType::Struct(fields), false)),
                        false,
                    ),
                    true,
                    50,
                ),
            )
            .unwrap();
            let map = output
                .column(0)
                .as_any()
                .downcast_ref::<MapArray>()
                .unwrap();
            assert_eq!(
                map.keys()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                vec![Some("東京"), Some("alpha"), Some("東京"), Some("alpha")]
            );
        }
    }

    #[test]
    fn closed_set_rejects_value_casts_and_unsupported_carriers_with_path() {
        for (array, dtype) in [
            (
                Arc::new(Int64Array::from(vec![Some(i64::MAX), None])) as ArrayRef,
                DataType::Int32,
            ),
            (
                Arc::new(
                    Decimal128Array::from(vec![100, 200])
                        .with_precision_and_scale(10, 2)
                        .unwrap(),
                ) as ArrayRef,
                DataType::Decimal128(9, 2),
            ),
            (
                Arc::new(
                    Decimal128Array::from(vec![100, 200])
                        .with_precision_and_scale(10, 2)
                        .unwrap(),
                ) as ArrayRef,
                DataType::Decimal128(10, 1),
            ),
            (
                Arc::new(
                    TimestampMicrosecondArray::from(vec![Some(1), None]).with_timezone("+08:00"),
                ) as ArrayRef,
                DataType::Timestamp(TimeUnit::Microsecond, None),
            ),
            (
                Arc::new(arrow::array::NullArray::new(2)) as ArrayRef,
                DataType::Utf8,
            ),
            (
                Arc::new(LargeStringArray::from(vec![Some("a"), None])) as ArrayRef,
                DataType::Utf8,
            ),
            (
                Arc::new(arrow::array::StringViewArray::from(vec![Some("a"), None])) as ArrayRef,
                DataType::Utf8,
            ),
            (
                Arc::new(
                    DictionaryArray::try_new(
                        Int16Array::from(vec![0, 0]),
                        Arc::new(StringArray::from(vec!["a"])) as ArrayRef,
                    )
                    .unwrap(),
                ) as ArrayRef,
                DataType::Utf8,
            ),
        ] {
            let error = adapt_one(array, field("bad", dtype, true, 60)).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("unsupported Iceberg write adaptation at `bad`"),
                "{error}"
            );
        }
    }

    #[test]
    fn required_root_checks_array_nulls_including_dictionary_value_nulls() {
        for array in [
            Arc::new(Int32Array::from(vec![Some(1), None])) as ArrayRef,
            dictionary(false, true),
        ] {
            let dtype = if matches!(array.data_type(), DataType::Dictionary(..)) {
                DataType::Utf8
            } else {
                DataType::Int32
            };
            let error = adapt_one(array, field("id", dtype, false, 1)).unwrap_err();
            assert!(error.to_string().contains("id"), "{error}");
        }
        // A non-null dictionary key can still select a NULL dictionary value.
        let array: ArrayRef = Arc::new(
            DictionaryArray::try_new(
                Int32Array::from(vec![0]),
                Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
            )
            .unwrap(),
        );
        assert!(adapt_one(array, field("id", DataType::Utf8, false, 1)).is_err());
    }

    #[test]
    fn nested_nullability_and_map_sort_mismatches_and_nested_narrow_ints_fail() {
        let array: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Int32, true)),
            OffsetBuffer::new(vec![0, 1].into()),
            Arc::new(Int32Array::from(vec![1])),
            None,
        ));
        let error = adapt_one(
            array,
            field(
                "l",
                DataType::List(Arc::new(field("element", DataType::Int32, false, 2))),
                true,
                1,
            ),
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("nested nullability mismatch at `l.element`"),
            "{error}"
        );
        let array: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Int8, true)),
            OffsetBuffer::new(vec![0, 1].into()),
            Arc::new(Int8Array::from(vec![1])),
            None,
        ));
        assert!(
            adapt_one(
                array,
                field(
                    "l",
                    DataType::List(Arc::new(field("element", DataType::Int32, true, 2))),
                    true,
                    1
                )
            )
            .unwrap_err()
            .to_string()
            .contains("l.element")
        );
        let fields: Fields = vec![
            Arc::new(Field::new("key", DataType::Int32, false)),
            Arc::new(Field::new("value", DataType::Int32, true)),
        ]
        .into();
        let entries = StructArray::new(
            fields.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(Int32Array::from(vec![2])),
            ],
            None,
        );
        let entry_field = Arc::new(Field::new("entries", DataType::Struct(fields), false));
        let array: ArrayRef = Arc::new(MapArray::new(
            entry_field.clone(),
            OffsetBuffer::new(vec![0, 1].into()),
            entries,
            None,
            false,
        ));
        let error =
            adapt_one(array, field("m", DataType::Map(entry_field, true), true, 1)).unwrap_err();
        assert!(error.to_string().contains("at `m`"), "{error}");
    }

    #[test]
    fn string_offset_overflow_is_checked_before_allocating() {
        assert_eq!(
            checked_utf8_total(i32::MAX as usize - 1, 1, "s").unwrap(),
            i32::MAX as usize
        );
        for (total, additional) in [(i32::MAX as usize, 1), (usize::MAX, 1)] {
            assert!(
                checked_utf8_total(total, additional, "s")
                    .unwrap_err()
                    .to_string()
                    .contains("offsets exceed i32 range at `s`")
            );
        }
    }

    #[test]
    fn reordered_top_level_names_do_not_rebind_values_and_identical_arrays_are_shared() {
        let values: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
        let input = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "input",
                DataType::Int32,
                true,
            )])),
            vec![values.clone()],
        )
        .unwrap();
        let error = adapt_batch(
            &input,
            &Arc::new(Schema::new(vec![field(
                "different",
                DataType::Int32,
                true,
                1,
            )])),
        )
        .unwrap_err();
        assert!(error.to_string().contains("field name mismatch"));
        let output = adapt_batch(
            &input,
            &Arc::new(Schema::new(vec![field("input", DataType::Int32, true, 1)])),
        )
        .unwrap();
        assert!(Arc::ptr_eq(&values, output.column(0)));
    }
    #[test]
    fn struct_child_names_are_semantic_and_must_not_rebind_equal_carriers() {
        let sources: Fields = vec![
            Arc::new(Field::new("right", DataType::Int32, true)),
            Arc::new(Field::new("left", DataType::Int32, true)),
        ]
        .into();
        let targets: Fields = vec![
            Arc::new(field("left", DataType::Int32, true, 2)),
            Arc::new(field("right", DataType::Int32, true, 3)),
        ]
        .into();
        let input_schema = Schema::new(vec![Field::new(
            "s",
            DataType::Struct(sources.clone()),
            true,
        )]);
        let target_schema = Schema::new(vec![field("s", DataType::Struct(targets), true, 1)]);
        let error = validate_batch_schema(&input_schema, &target_schema).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("struct field name mismatch at `s.left`"),
            "{error}"
        );
        let array = Arc::new(StructArray::new(
            sources,
            vec![
                Arc::new(Int32Array::from(vec![2])),
                Arc::new(Int32Array::from(vec![1])),
            ],
            None,
        ));
        let input = RecordBatch::try_new(Arc::new(input_schema), vec![array]).unwrap();
        assert!(
            adapt_batch(&input, &Arc::new(target_schema))
                .unwrap_err()
                .to_string()
                .contains("struct field name mismatch at `s.left`")
        );
    }
    #[test]
    fn fixed_width_values_preserve_bytes_nulls_and_identical_fixed_arrays() {
        let input: ArrayRef = Arc::new(BinaryArray::from(vec![
            Some(&b"abcdefghijklmnop"[..]),
            None,
        ]));
        let output =
            adapt_one(input, field("raw", DataType::FixedSizeBinary(16), true, 1)).unwrap();
        let fixed = output
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(fixed.value(0), b"abcdefghijklmnop");
        assert!(fixed.is_null(1));
        assert_eq!(fixed.value_length(), 16);
        let original = output.column(0).clone();
        let same = adapt_one(
            original.clone(),
            field("raw", DataType::FixedSizeBinary(16), true, 1),
        )
        .unwrap();
        assert!(Arc::ptr_eq(&original, same.column(0)));
    }

    #[test]
    fn fixed_width_preparation_accepts_byte_carriers_and_runtime_preserves_typed_length_failure() {
        for input in [DataType::Binary, DataType::FixedSizeBinary(3)] {
            validate_batch_schema(
                &Schema::new(vec![Field::new("raw", input, true)]),
                &Schema::new(vec![field("raw", DataType::FixedSizeBinary(4), true, 1)]),
            )
            .unwrap();
        }
        let mut builder = FixedSizeBinaryBuilder::with_capacity(1, 3);
        builder.append_value(b"abc").unwrap();
        let failure = adapt_one(
            Arc::new(builder.finish()),
            field("raw", DataType::FixedSizeBinary(4), true, 1),
        )
        .unwrap_err();
        assert!(
            matches!(failure, WriteAdaptationError::ValueWidth { ref path, expected: 4, actual: 3 } if path == "raw")
        );
        let failure = failure.into_connector_error();
        assert_eq!(failure.kind(), ConnectorErrorKind::InvalidRequest);
        assert!(failure.to_string().contains("requires 4 bytes, got 3"));
    }

    #[test]
    fn fixed_width_struct_values_ignore_parent_nulls_and_keep_required_placeholders_valid() {
        let sources: Fields = vec![Arc::new(Field::new("f", DataType::Binary, false))].into();
        let input: ArrayRef = Arc::new(
            StructArray::try_new(
                sources,
                vec![Arc::new(BinaryArray::from(vec![&b"abcd"[..], &b""[..]]))],
                Some(NullBuffer::from(vec![true, false])),
            )
            .unwrap(),
        );
        let targets: Fields =
            vec![Arc::new(field("f", DataType::FixedSizeBinary(4), false, 2))].into();
        let output = adapt_one(input, field("obj", DataType::Struct(targets), true, 1)).unwrap();
        let structure = output
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert!(structure.is_null(1));
        let fixed = structure
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(fixed.value(0), b"abcd");
        assert_eq!(fixed.value(1), &[0; 4]);
        assert!(!fixed.is_null(1));
        output.column(0).to_data().validate_full().unwrap();
    }

    #[test]
    fn fixed_width_sliced_list_only_checks_values_of_visible_non_null_parents() {
        let values: ArrayRef = Arc::new(BinaryArray::from(vec![
            &b"bad"[..],
            &b"abcd"[..],
            &b"hidden"[..],
            &b"wxyz"[..],
            &b"tail"[..],
        ]));
        let input: ArrayRef = Arc::new(
            ListArray::try_new(
                Arc::new(Field::new("item", DataType::Binary, false)),
                OffsetBuffer::new(vec![0, 1, 2, 3, 4].into()),
                values,
                Some(NullBuffer::from(vec![true, true, false, true])),
            )
            .unwrap(),
        );
        let input = input.slice(1, 3);
        let target_item = Arc::new(field("element", DataType::FixedSizeBinary(4), false, 2));
        let output = adapt_one(input, field("l", DataType::List(target_item), true, 1)).unwrap();
        let list = output
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(list.value_offsets(), &[1, 2, 3, 4]);
        assert!(list.is_null(1));
        let values = list
            .values()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(values.value(0), &[0; 4]);
        assert_eq!(values.value(1), b"abcd");
        assert_eq!(values.value(2), &[0; 4]);
        assert_eq!(values.value(3), b"wxyz");
        assert_eq!(values.value(4), &[0; 4]);
        assert_eq!(values.null_count(), 0);
        list.to_data().validate_full().unwrap();
    }

    #[test]
    fn fixed_width_sliced_map_keeps_offsets_and_only_converts_referenced_keys_and_values() {
        let fields: Fields = vec![
            Arc::new(Field::new("key", DataType::Binary, false)),
            Arc::new(Field::new("value", DataType::Binary, true)),
        ]
        .into();
        let entries = StructArray::try_new(
            fields.clone(),
            vec![
                Arc::new(BinaryArray::from(vec![
                    &b"bad"[..],
                    &b"hidden"[..],
                    &b"wrong"[..],
                    &b"key4"[..],
                    &b"tail"[..],
                ])),
                Arc::new(BinaryArray::from(vec![
                    Some(&b"bad"[..]),
                    None,
                    Some(&b"hidden"[..]),
                    Some(&b"ok"[..]),
                    Some(&b"tail"[..]),
                ])),
            ],
            None,
        )
        .unwrap();
        let input: ArrayRef = Arc::new(
            MapArray::try_new(
                Arc::new(Field::new("entries", DataType::Struct(fields), false)),
                OffsetBuffer::new(vec![1, 3, 4].into()),
                entries,
                Some(NullBuffer::from(vec![false, true])),
                false,
            )
            .unwrap(),
        );
        let fields: Fields = vec![
            Arc::new(field("key", DataType::FixedSizeBinary(4), false, 2)),
            Arc::new(field("value", DataType::FixedSizeBinary(2), true, 3)),
        ]
        .into();
        let output = adapt_one(
            input.slice(1, 1),
            field(
                "m",
                DataType::Map(
                    Arc::new(field("entries", DataType::Struct(fields), false, 4)),
                    false,
                ),
                true,
                1,
            ),
        )
        .unwrap();
        let map = output
            .column(0)
            .as_any()
            .downcast_ref::<MapArray>()
            .unwrap();
        assert_eq!(map.value_offsets(), &[3, 4]);
        let keys = map
            .keys()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(keys.value(3), b"key4");
        assert_eq!(keys.value(0), &[0; 4]);
        assert_eq!(keys.null_count(), 0);
        let values = map
            .values()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(values.value(3), b"ok");
        assert!(values.is_null(1));
        map.to_data().validate_full().unwrap();
    }

    #[test]
    fn fixed_width_reachable_nested_mismatch_reports_the_exact_field_path() {
        let source: Fields = vec![Arc::new(Field::new("f", DataType::Binary, true))].into();
        let input: ArrayRef = Arc::new(
            StructArray::try_new(
                source,
                vec![Arc::new(BinaryArray::from(vec![Some(&b"xx"[..])]))],
                None,
            )
            .unwrap(),
        );
        let target: Fields =
            vec![Arc::new(field("f", DataType::FixedSizeBinary(4), true, 2))].into();
        let failure =
            adapt_one(input, field("obj", DataType::Struct(target), true, 1)).unwrap_err();
        assert!(
            matches!(failure,WriteAdaptationError::ValueWidth { ref path, expected:4,actual:2 } if path == "obj.f")
        );
    }

    #[test]
    fn ordinary_same_type_structs_keep_the_original_array_without_child_work() {
        let fields: Fields = vec![Arc::new(Field::new("n", DataType::Int32, true))].into();
        let input: ArrayRef = Arc::new(
            StructArray::try_new(
                fields.clone(),
                vec![Arc::new(Int32Array::from(vec![Some(1), None]))],
                Some(NullBuffer::from(vec![true, false])),
            )
            .unwrap(),
        );
        let output = adapt_one(
            input.clone(),
            field("obj", DataType::Struct(fields), true, 1),
        )
        .unwrap();
        assert!(Arc::ptr_eq(&input, output.column(0)));
    }
}
