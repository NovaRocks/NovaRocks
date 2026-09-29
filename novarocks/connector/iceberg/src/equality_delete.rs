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

//! Iceberg equality-delete decoding and row visibility.
//!
//! Equality delete files are Iceberg physical facts.  The connector owns their
//! parquet decoding, field-ID matching, and scalar-key comparison; consumers
//! receive only the resulting row visibility decision.

use crate::delete_file::{IcebergDeleteFileSpec, IcebergFileContent, IcebergFileFormat};
use arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, RecordBatch,
    StringArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow::datatypes::{DataType, Field, SchemaRef, TimeUnit};
use novarocks_fs::{FileProjection, FileReadContext, FsAccessHandle};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use std::collections::HashSet;

/// The equality-delete files among `specs`, each checked to be one whole
/// Parquet file before any of them is read.
fn equality_specs(specs: &[IcebergDeleteFileSpec]) -> Result<Vec<&IcebergDeleteFileSpec>, String> {
    specs
        .iter()
        .filter(|spec| spec.file_content == IcebergFileContent::EqualityDeletes)
        .map(|spec| {
            if spec.file_format != IcebergFileFormat::Parquet
                || spec.content_offset.is_some()
                || spec.content_size_in_bytes.is_some()
            {
                return Err(format!(
                    "iceberg equality-delete file {} has unsupported physical layout",
                    spec.path
                ));
            }
            Ok(spec)
        })
        .collect()
}

fn parse_parquet_field_id(field: &Field) -> Result<Option<i32>, String> {
    let Some(raw) = field.metadata().get(PARQUET_FIELD_ID_META_KEY) else {
        return Ok(None);
    };
    raw.parse::<i32>().map(Some).map_err(|error| {
        format!(
            "invalid parquet field_id metadata: field={} key={} value={} error={}",
            field.name(),
            PARQUET_FIELD_ID_META_KEY,
            raw,
            error
        )
    })
}

fn array_as<T: 'static>(array: &dyn Array) -> Result<&T, String> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        format!(
            "array downcast failed for equality-delete filtering: {:?}",
            array.data_type()
        )
    })
}
/// Field/type-bound keys shared by all applications of one physical artifact.
pub(crate) type EqualityKey = Vec<Option<crate::delete_semantics::CanonicalScalar>>;

/// Owned by a split or one physical reader, never by a shared union.
pub(crate) struct EqualityColumnBinding {
    schema: SchemaRef,
    group: crate::delete_semantics::EqualityFieldGroup,
    indices: Box<[usize]>,
}
impl EqualityColumnBinding {
    pub(crate) fn bind(
        schema: SchemaRef,
        group: &crate::delete_semantics::EqualityFieldGroup,
    ) -> Result<Self, String> {
        let mut field_indices = std::collections::HashMap::new();
        for (index, field) in schema.fields().iter().enumerate() {
            if let Some(id) = parse_parquet_field_id(field)? {
                field_indices.entry(id).or_insert_with(Vec::new).push(index);
            }
        }
        let indices = group
            .fields()
            .iter()
            .map(|(id, ty)| {
                let index = match field_indices.get(id).map(Vec::as_slice) {
                    Some([index]) => *index,
                    _ => {
                        return Err(format!(
                            "Equality field ID {id} must bind exactly once in the projected batch"
                        ));
                    }
                };
                validate_physical_equality_type(schema.field(index).data_type(), ty)?;
                Ok(index)
            })
            .collect::<Result<Vec<_>, String>>()?
            .into_boxed_slice();
        Ok(Self {
            schema,
            group: group.clone(),
            indices,
        })
    }
    pub(crate) fn validate_schema(&self, schema: &SchemaRef) -> Result<(), String> {
        if std::sync::Arc::ptr_eq(&self.schema, schema) || self.schema == *schema {
            Ok(())
        } else {
            Err("Equality projected batch schema changed after column binding".to_string())
        }
    }
    pub(crate) fn keys(&self, batch: &RecordBatch) -> Result<Vec<EqualityKey>, String> {
        self.validate_schema(&batch.schema())?;
        self.keys_after_schema_validation(batch)
    }
    /// The split validates its common schema once before invoking each group.
    pub(crate) fn keys_after_schema_validation(
        &self,
        batch: &RecordBatch,
    ) -> Result<Vec<EqualityKey>, String> {
        (0..batch.num_rows())
            .map(|row| {
                self.group
                    .fields()
                    .iter()
                    .zip(self.indices.iter())
                    .map(|((_, ty), index)| {
                        canonical_equality_value(batch.column(*index).as_ref(), row, ty)
                    })
                    .collect()
            })
            .collect()
    }
}

#[cfg(test)]
pub(crate) fn bound_equality_keys(
    batch: &RecordBatch,
    group: &crate::delete_semantics::EqualityFieldGroup,
) -> Result<Vec<EqualityKey>, String> {
    EqualityColumnBinding::bind(batch.schema(), group)?.keys(batch)
}

fn validate_physical_equality_type(
    physical: &DataType,
    resolved: &crate::iceberg::spec::PrimitiveType,
) -> Result<(), String> {
    use crate::iceberg::spec::PrimitiveType as P;
    let valid = match (resolved, physical) {
        (P::Boolean, DataType::Boolean)
        | (P::Int, DataType::Int8 | DataType::Int16 | DataType::Int32)
        | (P::Long, DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64)
        | (P::Float, DataType::Float32)
        | (P::Double, DataType::Float32 | DataType::Float64)
        | (P::Date, DataType::Date32)
        | (P::Time, DataType::Time64(TimeUnit::Microsecond))
        | (P::String, DataType::Utf8 | DataType::LargeUtf8)
        | (P::Binary, DataType::Binary | DataType::LargeBinary)
        | (P::Uuid, DataType::FixedSizeBinary(16))
        | (
            P::Timestamp | P::Timestamptz | P::TimestampNs | P::TimestamptzNs,
            DataType::Timestamp(_, _),
        ) => true,
        (P::Fixed(expected), DataType::FixedSizeBinary(actual)) => {
            i64::try_from(*expected).ok() == Some(i64::from(*actual))
        }
        (P::Decimal { precision, scale }, DataType::Decimal128(actual_precision, actual_scale)) => {
            u32::from(*actual_precision) <= *precision
                && i32::try_from(*scale).ok() == Some(i32::from(*actual_scale))
        }
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(format!(
            "Equality physical type {physical:?} does not match resolved Iceberg type {resolved:?}"
        ))
    }
}

fn canonical_equality_value(
    array: &dyn Array,
    row: usize,
    ty: &crate::iceberg::spec::PrimitiveType,
) -> Result<Option<crate::delete_semantics::CanonicalScalar>, String> {
    use crate::delete_semantics::CanonicalScalar as C;
    use crate::iceberg::spec::PrimitiveType as P;
    use arrow::array::{FixedSizeBinaryArray, Time64MicrosecondArray};
    use std::sync::Arc;
    if array.is_null(row) {
        return Ok(None);
    }
    let invalid = || {
        format!(
            "Equality physical type {:?} does not match resolved Iceberg type {ty:?}",
            array.data_type()
        )
    };
    let value = match (ty, array.data_type()) {
        (P::Boolean, DataType::Boolean) => C::Boolean(array_as::<BooleanArray>(array)?.value(row)),
        (P::Int, DataType::Int8) => C::Int(i32::from(array_as::<Int8Array>(array)?.value(row))),
        (P::Int, DataType::Int16) => C::Int(i32::from(array_as::<Int16Array>(array)?.value(row))),
        (P::Int, DataType::Int32) => C::Int(array_as::<Int32Array>(array)?.value(row)),
        (P::Long, DataType::Int8) => C::Long(i64::from(array_as::<Int8Array>(array)?.value(row))),
        (P::Long, DataType::Int16) => C::Long(i64::from(array_as::<Int16Array>(array)?.value(row))),
        (P::Long, DataType::Int32) => C::Long(i64::from(array_as::<Int32Array>(array)?.value(row))),
        (P::Long, DataType::Int64) => C::Long(array_as::<Int64Array>(array)?.value(row)),
        (P::Date, DataType::Date32) => C::Int(array_as::<Date32Array>(array)?.value(row)),
        (P::Time, DataType::Time64(TimeUnit::Microsecond)) => {
            C::Long(array_as::<Time64MicrosecondArray>(array)?.value(row))
        }
        (P::Float, DataType::Float32) => {
            let value = array_as::<Float32Array>(array)?.value(row);
            C::Float(if value.is_nan() {
                f32::NAN.to_bits()
            } else {
                value.to_bits()
            })
        }
        (P::Double, DataType::Float32 | DataType::Float64) => {
            let value = match array.data_type() {
                DataType::Float32 => f64::from(array_as::<Float32Array>(array)?.value(row)),
                _ => array_as::<Float64Array>(array)?.value(row),
            };
            C::Double(if value.is_nan() {
                f64::NAN.to_bits()
            } else {
                value.to_bits()
            })
        }
        (P::String, DataType::Utf8) => {
            C::String(Arc::from(array_as::<StringArray>(array)?.value(row)))
        }
        (P::String, DataType::LargeUtf8) => {
            C::String(Arc::from(array_as::<LargeStringArray>(array)?.value(row)))
        }
        (P::Binary, DataType::Binary) => {
            C::Binary(Arc::from(array_as::<BinaryArray>(array)?.value(row)))
        }
        (P::Binary, DataType::LargeBinary) => {
            C::Binary(Arc::from(array_as::<LargeBinaryArray>(array)?.value(row)))
        }
        (P::Fixed(length), DataType::FixedSizeBinary(actual))
            if i64::try_from(*length).ok() == Some(i64::from(*actual)) =>
        {
            C::Binary(Arc::from(
                array_as::<FixedSizeBinaryArray>(array)?.value(row),
            ))
        }
        (P::Uuid, DataType::FixedSizeBinary(16)) => C::Uuid(u128::from_be_bytes(
            array_as::<FixedSizeBinaryArray>(array)?
                .value(row)
                .try_into()
                .map_err(|_| invalid())?,
        )),
        (P::Decimal { scale, .. }, DataType::Decimal128(_, actual))
            if i32::try_from(*scale).ok() == Some(i32::from(*actual)) =>
        {
            C::Decimal(array_as::<Decimal128Array>(array)?.value(row))
        }
        (
            P::Timestamp | P::Timestamptz | P::TimestampNs | P::TimestamptzNs,
            DataType::Timestamp(unit, _),
        ) => {
            let raw = match unit {
                TimeUnit::Second => array_as::<TimestampSecondArray>(array)?.value(row),
                TimeUnit::Millisecond => array_as::<TimestampMillisecondArray>(array)?.value(row),
                TimeUnit::Microsecond => array_as::<TimestampMicrosecondArray>(array)?.value(row),
                TimeUnit::Nanosecond => array_as::<TimestampNanosecondArray>(array)?.value(row),
            };
            let source = match unit {
                TimeUnit::Second => 1,
                TimeUnit::Millisecond => 1_000,
                TimeUnit::Microsecond => 1_000_000,
                TimeUnit::Nanosecond => 1_000_000_000,
            };
            let target = if matches!(ty, P::TimestampNs | P::TimestamptzNs) {
                1_000_000_000
            } else {
                1_000_000
            };
            C::Long(if source <= target {
                raw.checked_mul(target / source).ok_or_else(invalid)?
            } else {
                if raw % (source / target) != 0 {
                    return Err(invalid());
                }
                raw / (source / target)
            })
        }
        _ => return Err(invalid()),
    };
    Ok(Some(value))
}

pub(crate) async fn load_bound_equality_keys(
    spec: &IcebergDeleteFileSpec,
    group: &crate::delete_semantics::EqualityFieldGroup,
    access: &FsAccessHandle,
    context: &FileReadContext,
    mut on_decoded: impl FnMut(usize) + Send,
) -> Result<(Vec<EqualityKey>, usize), novarocks_spi::connector::ConnectorError> {
    equality_specs(std::slice::from_ref(spec))
        .map_err(crate::file_reader::corrupt_delete_content)?;
    let mut keys = HashSet::new();
    let mut decoded_rows = 0usize;
    let mut columns = None;
    crate::file_reader::visit_parquet_batches_async(
        access,
        &spec.path,
        spec.length,
        FileProjection::FieldIds(group.fields().iter().map(|(id, _)| *id).collect()),
        Vec::new(),
        context.clone(),
        |batch| {
            decoded_rows += batch.batch.num_rows();
            on_decoded(batch.batch.num_rows());
            if columns.is_none() {
                columns = Some(
                    EqualityColumnBinding::bind(batch.batch.schema(), group)
                        .map_err(crate::file_reader::corrupt_delete_content)?,
                );
            }
            keys.extend(
                columns
                    .as_ref()
                    .expect("bound physical reader columns")
                    .keys(&batch.batch)
                    .map_err(crate::file_reader::corrupt_delete_content)?,
            );
            Ok(())
        },
    )
    .await?;
    // No application is merged before the entire artifact validated.
    Ok((keys.into_iter().collect(), decoded_rows))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delete_semantics::{CanonicalScalar, EqualityFieldGroup};
    use crate::iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use arrow::array::{ArrayRef, FixedSizeBinaryArray};
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn physical_reader_binding_reuses_indices_and_rejects_changed_batch_schema() {
        let group = group(&[1], &[PrimitiveType::Long]);
        let first = batch(vec![
            (2, Arc::new(StringArray::from(vec!["x"]))),
            (1, Arc::new(Int64Array::from(vec![7]))),
        ]);
        let bound = EqualityColumnBinding::bind(first.schema(), &group).unwrap();
        assert_eq!(
            bound.keys(&first).unwrap(),
            vec![vec![Some(CanonicalScalar::Long(7))]]
        );
        let next = RecordBatch::try_new(
            Arc::new(first.schema().as_ref().clone()),
            vec![
                Arc::new(StringArray::from(vec!["y"])),
                Arc::new(Int64Array::from(vec![8])),
            ],
        )
        .unwrap();
        assert_eq!(
            bound.keys(&next).unwrap(),
            vec![vec![Some(CanonicalScalar::Long(8))]]
        );
        let reordered = batch(vec![
            (1, Arc::new(Int64Array::from(vec![7]))),
            (2, Arc::new(StringArray::from(vec!["x"]))),
        ]);
        assert!(
            bound
                .keys(&reordered)
                .unwrap_err()
                .contains("schema changed")
        );
        let widened = batch(vec![
            (2, Arc::new(StringArray::from(vec!["x"]))),
            (1, Arc::new(Int32Array::from(vec![7]))),
        ]);
        assert!(bound.keys(&widened).unwrap_err().contains("schema changed"));
    }

    fn group(ids: &[i32], types: &[PrimitiveType]) -> EqualityFieldGroup {
        let schema = Schema::builder()
            .with_fields(
                ids.iter()
                    .zip(types)
                    .map(|(id, ty)| {
                        Arc::new(NestedField::optional(
                            *id,
                            format!("column_{id}"),
                            Type::Primitive(ty.clone()),
                        ))
                    })
                    .collect::<Vec<_>>(),
            )
            .build()
            .unwrap();
        EqualityFieldGroup::bind(ids, &schema).unwrap()
    }
    fn batch(fields: Vec<(i32, ArrayRef)>) -> RecordBatch {
        let schema = arrow::datatypes::Schema::new(
            fields
                .iter()
                .map(|(id, array)| {
                    Field::new(format!("renamed_{id}"), array.data_type().clone(), true)
                        .with_metadata(HashMap::from([(
                            PARQUET_FIELD_ID_META_KEY.to_string(),
                            id.to_string(),
                        )]))
                })
                .collect::<Vec<_>>(),
        );
        RecordBatch::try_new(
            Arc::new(schema),
            fields.into_iter().map(|(_, a)| a).collect(),
        )
        .unwrap()
    }
    #[test]
    fn canonical_keys_preserve_null_signed_zero_and_canonicalize_nan_payloads() {
        let g = group(&[4], &[PrimitiveType::Double]);
        let values = Float64Array::from(vec![
            None,
            Some(0.0),
            Some(-0.0),
            Some(f64::from_bits(0x7ff8_0000_0000_0001)),
            Some(f64::from_bits(0xfff8_0000_0000_0123)),
        ]);
        let keys = bound_equality_keys(&batch(vec![(4, Arc::new(values))]), &g).unwrap();
        assert_eq!(keys[0], vec![None]);
        assert_ne!(keys[1], keys[2]);
        assert_eq!(keys[3], keys[4]);
        assert_ne!(keys[0], keys[3]);
    }
    #[test]
    fn promoted_historical_and_current_physical_keys_are_identical() {
        let g = group(&[1, 4], &[PrimitiveType::Long, PrimitiveType::Double]);
        let old = batch(vec![
            (
                4,
                Arc::new(Float32Array::from(vec![Some(1.5), None, Some(f32::NAN)])),
            ),
            (1, Arc::new(Int32Array::from(vec![7, 8, 9]))),
        ]);
        let current = batch(vec![
            (1, Arc::new(Int64Array::from(vec![7, 8, 9]))),
            (
                4,
                Arc::new(Float64Array::from(vec![Some(1.5), None, Some(f64::NAN)])),
            ),
        ]);
        assert_eq!(
            bound_equality_keys(&old, &g).unwrap(),
            bound_equality_keys(&current, &g).unwrap()
        );
    }
    #[test]
    fn field_binding_never_falls_back_to_name_or_ambiguous_id() {
        let g = group(&[1], &[PrimitiveType::Long]);
        let missing = RecordBatch::try_new(
            Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
                "column_1",
                DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![7]))],
        )
        .unwrap();
        assert!(
            bound_equality_keys(&missing, &g)
                .unwrap_err()
                .contains("exactly once")
        );
        let duplicate = batch(vec![
            (1, Arc::new(Int64Array::from(vec![7]))),
            (1, Arc::new(Int64Array::from(vec![7]))),
        ]);
        assert!(
            bound_equality_keys(&duplicate, &g)
                .unwrap_err()
                .contains("exactly once")
        );
        let wrong = batch(vec![(1, Arc::new(StringArray::from(vec!["7"])))]);
        assert!(bound_equality_keys(&wrong, &g).is_err());
        let all_null_wrong_type = batch(vec![(1, Arc::new(StringArray::from(vec![None::<&str>])))]);
        assert!(bound_equality_keys(&all_null_wrong_type, &g).is_err());
        let empty_wrong_type = batch(vec![(1, Arc::new(StringArray::from(Vec::<&str>::new())))]);
        assert!(bound_equality_keys(&empty_wrong_type, &g).is_err());
    }
    #[test]
    fn uuid_decimal_and_time_keys_respect_bound_physical_semantics() {
        let uuid = FixedSizeBinaryArray::try_from_iter([&[1u8; 16][..]].into_iter()).unwrap();
        let decimal = Decimal128Array::from(vec![123i128])
            .with_precision_and_scale(10, 2)
            .unwrap();
        let b = batch(vec![
            (1, Arc::new(uuid)),
            (2, Arc::new(decimal)),
            (3, Arc::new(TimestampMillisecondArray::from(vec![123]))),
        ]);
        let g = group(
            &[1, 2, 3],
            &[
                PrimitiveType::Uuid,
                PrimitiveType::Decimal {
                    precision: 10,
                    scale: 2,
                },
                PrimitiveType::Timestamp,
            ],
        );
        assert_eq!(
            bound_equality_keys(&b, &g).unwrap(),
            vec![vec![
                Some(CanonicalScalar::Uuid(u128::from_be_bytes([1; 16]))),
                Some(CanonicalScalar::Decimal(123)),
                Some(CanonicalScalar::Long(123_000))
            ]]
        );
        let wrong_scale = batch(vec![(
            2,
            Arc::new(
                Decimal128Array::from(vec![123])
                    .with_precision_and_scale(10, 3)
                    .unwrap(),
            ),
        )]);
        assert!(
            bound_equality_keys(
                &wrong_scale,
                &group(
                    &[2],
                    &[PrimitiveType::Decimal {
                        precision: 10,
                        scale: 2
                    }]
                )
            )
            .is_err()
        );
        let precision_loss = batch(vec![(
            3,
            Arc::new(TimestampNanosecondArray::from(vec![1001])),
        )]);
        assert!(
            bound_equality_keys(&precision_loss, &group(&[3], &[PrimitiveType::Timestamp]))
                .is_err()
        );
        let overflow = batch(vec![(
            3,
            Arc::new(TimestampSecondArray::from(vec![i64::MAX])),
        )]);
        assert!(bound_equality_keys(&overflow, &group(&[3], &[PrimitiveType::Timestamp])).is_err());
    }
    #[test]
    fn scalar_integer_semantic_pages_match_storage_int_canonical_keys() {
        for resolved in [PrimitiveType::Int, PrimitiveType::Long] {
            let g = group(&[17], &[resolved]);
            let storage = batch(vec![(
                17,
                Arc::new(Int32Array::from(vec![Some(-128), None])),
            )]);
            let deleted = bound_equality_keys(&storage, &g).unwrap();
            let delete_set = deleted.iter().cloned().collect::<HashSet<_>>();
            for array in [
                Arc::new(Int8Array::from(vec![Some(-128), Some(127), None])) as ArrayRef,
                Arc::new(Int16Array::from(vec![Some(-128), Some(127), None])) as ArrayRef,
            ] {
                let page = batch(vec![(17, array)]);
                let keys = EqualityColumnBinding::bind(page.schema(), &g)
                    .unwrap()
                    .keys(&page)
                    .unwrap();
                assert_eq!(keys[0], deleted[0]);
                assert_eq!(keys[2], deleted[1]);
                assert_eq!(
                    keys.iter()
                        .map(|key| !delete_set.contains(key))
                        .collect::<Vec<_>>(),
                    vec![false, true, false]
                );
            }
        }
    }
}
