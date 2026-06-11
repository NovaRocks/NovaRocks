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

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Decimal128Array, Decimal256Array, make_array};
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

pub(crate) fn is_execution_data_type_compatible(expected: &DataType, actual: &DataType) -> bool {
    if expected == actual {
        return true;
    }

    match (expected, actual) {
        (DataType::Decimal128(_, expected_scale), DataType::Decimal128(_, actual_scale)) => {
            expected_scale == actual_scale
        }
        (DataType::Decimal256(_, expected_scale), DataType::Decimal256(_, actual_scale)) => {
            expected_scale == actual_scale
        }
        (DataType::Timestamp(_, _), DataType::Timestamp(_, _)) => true,
        (DataType::Utf8, DataType::Binary) | (DataType::Binary, DataType::Utf8) => true,
        (DataType::List(expected_field), DataType::List(actual_field)) => {
            is_execution_data_type_compatible(expected_field.data_type(), actual_field.data_type())
        }
        (DataType::LargeList(expected_field), DataType::LargeList(actual_field)) => {
            is_execution_data_type_compatible(expected_field.data_type(), actual_field.data_type())
        }
        (
            DataType::Map(expected_field, expected_ordered),
            DataType::Map(actual_field, actual_ordered),
        ) => {
            expected_ordered == actual_ordered
                && is_execution_data_type_compatible(
                    expected_field.data_type(),
                    actual_field.data_type(),
                )
        }
        (DataType::List(_), DataType::Struct(actual_fields)) if actual_fields.len() == 1 => {
            is_execution_data_type_compatible(expected, actual_fields[0].data_type())
        }
        (DataType::Struct(expected_fields), DataType::List(_)) if expected_fields.len() == 1 => {
            is_execution_data_type_compatible(expected_fields[0].data_type(), actual)
        }
        (DataType::Struct(expected_fields), DataType::Struct(actual_fields)) => {
            expected_fields.len() == actual_fields.len()
                && expected_fields.iter().zip(actual_fields.iter()).all(
                    |(expected_field, actual_field)| {
                        is_execution_data_type_compatible(
                            expected_field.data_type(),
                            actual_field.data_type(),
                        )
                    },
                )
        }
        _ => false,
    }
}

fn align_field_to_data_type(
    field: &Field,
    actual_type: &DataType,
    actual_nullable: bool,
    context: &str,
) -> Result<Field, String> {
    if !is_execution_data_type_compatible(field.data_type(), actual_type) {
        return Err(format!(
            "{context} schema field type mismatch: expected {:?}, got {:?}",
            field.data_type(),
            actual_type
        ));
    }
    if field.data_type() == actual_type && (field.is_nullable() || !actual_nullable) {
        return Ok(field.clone());
    }
    Ok(Field::new(
        field.name(),
        actual_type.clone(),
        field.is_nullable() || actual_nullable,
    )
    .with_metadata(field.metadata().clone()))
}

pub(crate) fn align_fields_to_arrays(
    fields: &Fields,
    arrays: &[ArrayRef],
    context: &str,
) -> Result<Fields, String> {
    if fields.len() != arrays.len() {
        return Err(format!(
            "{context} schema/array length mismatch: schema_fields={} arrays={}",
            fields.len(),
            arrays.len()
        ));
    }
    let fields = fields
        .iter()
        .zip(arrays.iter())
        .map(|(field, array)| {
            align_field_to_data_type(
                field.as_ref(),
                array.data_type(),
                array.null_count() > 0,
                context,
            )
            .map(Arc::new)
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(fields.into())
}

pub(crate) fn align_schema_to_arrays(
    schema: &SchemaRef,
    arrays: &[ArrayRef],
    context: &str,
) -> Result<SchemaRef, String> {
    let fields = align_fields_to_arrays(schema.fields(), arrays, context)?;
    if fields == *schema.fields() {
        return Ok(Arc::clone(schema));
    }
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        schema.metadata().clone(),
    )))
}

pub(crate) fn align_schema_to_batches(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    context: &str,
) -> Result<SchemaRef, String> {
    let mut aligned = Arc::clone(schema);
    for batch in batches {
        aligned = align_schema_to_arrays(&aligned, batch.columns(), context)?;
    }
    Ok(aligned)
}

fn retag_decimal128_array(
    array: &Decimal128Array,
    target_precision: u8,
    target_scale: i8,
) -> Result<ArrayRef, String> {
    let data = array
        .to_data()
        .into_builder()
        .data_type(DataType::Decimal128(target_precision, target_scale))
        .build()
        .map_err(|e| format!("retag Decimal128 array failed: {e}"))?;
    Ok(make_array(data))
}

fn retag_decimal256_array(
    array: &Decimal256Array,
    target_precision: u8,
    target_scale: i8,
) -> Result<ArrayRef, String> {
    let data = array
        .to_data()
        .into_builder()
        .data_type(DataType::Decimal256(target_precision, target_scale))
        .build()
        .map_err(|e| format!("retag Decimal256 array failed: {e}"))?;
    Ok(make_array(data))
}

pub(crate) fn normalize_array_to_data_type(
    array: &ArrayRef,
    target_type: &DataType,
    context: &str,
) -> Result<ArrayRef, String> {
    if array.data_type() == target_type {
        return Ok(array.clone());
    }
    match (array.data_type(), target_type) {
        (
            DataType::Decimal128(_, source_scale),
            DataType::Decimal128(target_precision, target_scale),
        ) if source_scale == target_scale => {
            let decimal = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| format!("{context} Decimal128 downcast failed"))?;
            retag_decimal128_array(decimal, *target_precision, *target_scale)
        }
        (
            DataType::Decimal256(_, source_scale),
            DataType::Decimal256(target_precision, target_scale),
        ) if source_scale == target_scale => {
            let decimal = array
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .ok_or_else(|| format!("{context} Decimal256 downcast failed"))?;
            retag_decimal256_array(decimal, *target_precision, *target_scale)
        }
        _ if is_execution_data_type_compatible(target_type, array.data_type()) => Ok(array.clone()),
        _ => Err(format!(
            "{context} array type mismatch: expected {:?}, got {:?}",
            target_type,
            array.data_type()
        )),
    }
}

pub(crate) fn normalize_batch_to_schema(
    schema: &SchemaRef,
    batch: &RecordBatch,
    context: &str,
) -> Result<RecordBatch, String> {
    if schema.fields().len() != batch.num_columns() {
        return Err(format!(
            "{context} schema/batch length mismatch: schema_fields={} batch_columns={}",
            schema.fields().len(),
            batch.num_columns()
        ));
    }
    let columns = schema
        .fields()
        .iter()
        .zip(batch.columns().iter())
        .map(|(field, array)| normalize_array_to_data_type(array, field.data_type(), context))
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new(Arc::clone(schema), columns).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Decimal128Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use arrow::compute::concat_batches;

    use super::{
        align_schema_to_arrays, align_schema_to_batches, is_execution_data_type_compatible,
        normalize_batch_to_schema,
    };

    #[test]
    fn decimal_precision_is_compatible_only_when_scale_matches() {
        assert!(is_execution_data_type_compatible(
            &DataType::Decimal128(10, 2),
            &DataType::Decimal128(38, 2)
        ));
        assert!(!is_execution_data_type_compatible(
            &DataType::Decimal128(10, 2),
            &DataType::Decimal128(38, 3)
        ));
    }

    #[test]
    fn align_schema_to_arrays_uses_actual_decimal_precision() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "v",
            DataType::Decimal128(10, 2),
            false,
        )]));
        let array = Arc::new(
            Decimal128Array::from(vec![Some(123_i128)])
                .with_precision_and_scale(38, 2)
                .expect("decimal type"),
        ) as ArrayRef;

        let aligned = align_schema_to_arrays(&schema, &[array], "test").expect("align schema");
        assert_eq!(aligned.field(0).data_type(), &DataType::Decimal128(38, 2));
    }

    #[test]
    fn normalize_batch_to_schema_retags_decimal_for_concat() {
        let narrow_schema = Arc::new(Schema::new(vec![Field::new(
            "v",
            DataType::Decimal128(8, 2),
            true,
        )]));
        let wide_schema = Arc::new(Schema::new(vec![Field::new(
            "v",
            DataType::Decimal128(38, 2),
            true,
        )]));
        let narrow = RecordBatch::try_new(
            narrow_schema.clone(),
            vec![Arc::new(
                Decimal128Array::from(vec![Some(123_i128)])
                    .with_precision_and_scale(8, 2)
                    .expect("decimal type"),
            ) as ArrayRef],
        )
        .expect("narrow batch");
        let wide = RecordBatch::try_new(
            wide_schema,
            vec![Arc::new(
                Decimal128Array::from(vec![Some(456_i128)])
                    .with_precision_and_scale(38, 2)
                    .expect("decimal type"),
            ) as ArrayRef],
        )
        .expect("wide batch");

        let schema =
            align_schema_to_batches(&narrow_schema, &[narrow.clone(), wide.clone()], "test")
                .expect("align schema");
        let narrow = normalize_batch_to_schema(&schema, &narrow, "test").expect("normalize");
        let wide = normalize_batch_to_schema(&schema, &wide, "test").expect("normalize");
        concat_batches(&schema, [&narrow, &wide]).expect("concat");
    }
}
