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
//! Sorter kernels used by sort operators.
//!
//! Responsibilities:
//! - Host reusable sorter implementations so the pipeline operator can choose
//!   full-sort vs. top-n behavior explicitly.
//! - Keep sorting algorithms isolated from operator state transitions.

#[cfg(test)]
use crate::exec::chunk::Chunk;
#[cfg(test)]
use crate::exec::chunk::type_compatibility::check_exact;
#[cfg(test)]
use crate::exec::chunk::type_compatibility::retag_column;
use arrow::array::{
    Array, ArrayRef, Decimal128Array, FixedSizeBinaryArray, FixedSizeListArray, Int8Array,
    LargeListArray, ListArray, MapArray, StructArray, UInt64Array,
};
use arrow::compute::{SortColumn, SortOptions};
#[cfg(test)]
use arrow::datatypes::Schema;
#[cfg(test)]
use arrow::datatypes::SchemaRef;
use arrow::datatypes::{DataType, Field, Fields};
#[cfg(test)]
use arrow::record_batch::RecordBatch;
use arrow_buffer::OffsetBuffer;
use std::sync::Arc;

use novarocks_types::largeint;

mod key_failure;
mod operation_failure;
pub(crate) use key_failure::SortKeyFailure;

pub(super) mod creation;

#[cfg(test)]
fn sort_field_from_array(field: &Field, array: &ArrayRef) -> Field {
    Field::new(
        field.name(),
        array.data_type().clone(),
        field.is_nullable() || array.null_count() > 0,
    )
    .with_metadata(field.metadata().clone())
}

#[cfg(test)]
pub(crate) fn merged_sort_schema_for_chunks(chunks: &[Chunk]) -> Result<SchemaRef, String> {
    let first_schema = chunks
        .first()
        .ok_or_else(|| "sort schema merge requires non-empty chunks".to_string())?
        .schema();
    let field_count = first_schema.fields().len();
    let mut fields = first_schema
        .fields()
        .iter()
        .zip(chunks[0].record_batch().columns().iter())
        .map(|(field, array)| sort_field_from_array(field.as_ref(), array))
        .collect::<Vec<_>>();
    let mut changed = first_schema
        .fields()
        .iter()
        .zip(fields.iter())
        .any(|(original, merged)| original.as_ref() != merged);

    for chunk in chunks {
        let schema = chunk.schema();
        if schema.fields().len() != field_count {
            return Err(format!(
                "sort schema field count mismatch: expected={} actual={}",
                field_count,
                schema.fields().len()
            ));
        }
        for (idx, expected) in fields.clone().iter().enumerate().take(field_count) {
            let actual = schema.field(idx);
            let actual = sort_field_from_array(actual, chunk.record_batch().column(idx));
            if expected.name() != actual.name()
                || check_exact(expected.data_type(), actual.data_type()).is_err()
            {
                return Err(format!(
                    "sort schema field mismatch at index {}: expected=({}, {:?}) actual=({}, {:?})",
                    idx,
                    expected.name(),
                    expected.data_type(),
                    actual.name(),
                    actual.data_type()
                ));
            }
            let nullable = expected.is_nullable()
                || actual.is_nullable()
                || chunk.record_batch().column(idx).null_count() > 0;
            if nullable != expected.is_nullable() {
                fields[idx] = expected.clone().with_nullable(nullable);
                changed = true;
            }
        }
    }

    if !changed {
        return Ok(first_schema);
    }
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        first_schema.metadata().clone(),
    )))
}

#[cfg(test)]
fn normalize_sort_array_for_field(array: &ArrayRef, field: &Field) -> Result<ArrayRef, String> {
    if let Err(mismatch) = check_exact(field.data_type(), array.data_type()) {
        return Err(format!(
            "sort payload type mismatch for field {}: array={:?} field={:?} ({:?})",
            field.name(),
            array.data_type(),
            field.data_type(),
            mismatch.kind
        ));
    }
    retag_column(array, field.data_type()).map_err(|m| {
        format!(
            "sort payload retag failed for field {}: array={:?} field={:?} ({:?})",
            field.name(),
            array.data_type(),
            field.data_type(),
            m.kind
        )
    })
}

#[cfg(test)]
pub(crate) fn normalize_sort_batch_for_schema(
    chunk: &Chunk,
    schema: &SchemaRef,
    chunk_index: usize,
) -> Result<RecordBatch, String> {
    if chunk.record_batch().num_columns() != schema.fields().len() {
        return Err(format!(
            "sort chunk column count mismatch at index {}: batch_columns={} schema_fields={}",
            chunk_index,
            chunk.record_batch().num_columns(),
            schema.fields().len()
        ));
    }
    let columns = chunk
        .record_batch()
        .columns()
        .iter()
        .zip(schema.fields().iter())
        .map(|(array, field)| normalize_sort_array_for_field(array, field.as_ref()))
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new(Arc::clone(schema), columns).map_err(|e| {
        format!(
            "failed to normalize sort chunk schema at index {}: {e}",
            chunk_index
        )
    })
}

#[cfg(test)]
pub(crate) fn normalize_sort_key_array(values: &ArrayRef) -> Result<ArrayRef, SortKeyFailure> {
    normalize_sort_key_array_body::<SortKeyFailure>(
        values,
        SortKeyDeclaration::LegacyExpansion,
        &mut |_| Ok(()),
    )
}

pub(crate) enum SortKeyConstruction<'a> {
    ArrayHeader(&'a DataType),
    ArrayTable(usize),
    FieldTable(usize),
    SharedFields(usize),
    BufferAdoption,
    Field {
        source: &'a Field,
        target: &'a DataType,
    },
    Work(usize),
}

enum SortKeyTypeCheckFailure<E> {
    Declaration(novarocks_type_contract::ExecutionValueTypeError),
    Observer(E),
}
impl<E> From<novarocks_type_contract::ExecutionValueTypeError> for SortKeyTypeCheckFailure<E> {
    fn from(error: novarocks_type_contract::ExecutionValueTypeError) -> Self {
        Self::Declaration(error)
    }
}

#[derive(Clone, Copy)]
enum SortKeyDeclaration<'a> {
    Value(&'a novarocks_type_contract::ValueTypeRef),
    // Map entries are a physical Struct wrapper, not an independently authored
    // semantic Struct. Borrow the two real anonymous Map roles directly.
    MapEntries(
        &'a novarocks_type_contract::ValueTypeRef,
        &'a novarocks_type_contract::ValueTypeRef,
    ),
    #[cfg(test)]
    LegacyExpansion,
}
impl<'a> SortKeyDeclaration<'a> {
    fn child(self, index: usize) -> Result<Self, SortKeyFailure> {
        match self {
            Self::Value(value) => value
                .child_type_at(index)
                .map(Self::Value)
                .ok_or_else(|| SortKeyFailure::MissingChild { index }),
            Self::MapEntries(key, _) if index == 0 => Ok(Self::Value(key)),
            Self::MapEntries(_, value) if index == 1 => Ok(Self::Value(value)),
            Self::MapEntries(..) => Err(SortKeyFailure::MapChild { index }),
            #[cfg(test)]
            Self::LegacyExpansion => Ok(Self::LegacyExpansion),
        }
    }
    fn map_entries(self) -> Result<Self, SortKeyFailure> {
        match self {
            Self::Value(value) => match value.kind() {
                novarocks_type_contract::ValueKind::Map { key, value } => {
                    Ok(Self::MapEntries(key, value))
                }
                _ => Err(SortKeyFailure::MapDeclaration),
            },
            #[cfg(test)]
            Self::LegacyExpansion => Ok(Self::LegacyExpansion),
            _ => Err(SortKeyFailure::MapWrapper),
        }
    }
    fn is_largeint(self, _carrier: &DataType) -> bool {
        match self {
            Self::Value(value) => {
                value.domain() == Some(novarocks_type_contract::ValueLogicalType::LargeInt)
            }
            Self::MapEntries(..) => false,
            #[cfg(test)]
            Self::LegacyExpansion => largeint::is_largeint_data_type(_carrier),
        }
    }
}

/// The original key normalization body with observations before its real
/// public structure constructors. Existing decoded payload algorithms stay
/// here; their allocator/accounting is not replaced by this header observer.
pub(crate) fn normalize_sort_key_array_observed<E: From<SortKeyFailure>>(
    values: &ArrayRef,
    declared: Option<&novarocks_type_contract::ValueTypeRef>,
    observe: &mut impl FnMut(SortKeyConstruction<'_>) -> Result<(), E>,
) -> Result<ArrayRef, E> {
    let declared = declared.ok_or_else(|| E::from(SortKeyFailure::MissingDeclaration))?;
    novarocks_type_contract::validate_value_layout_observed::<SortKeyTypeCheckFailure<E>>(
        declared,
        values.data_type(),
        |units| {
            observe(SortKeyConstruction::Work(units)).map_err(SortKeyTypeCheckFailure::Observer)
        },
    )
    .map_err(|cause| match cause {
        SortKeyTypeCheckFailure::Declaration(error) => E::from(SortKeyFailure::Type(error)),
        SortKeyTypeCheckFailure::Observer(error) => error,
    })?;
    normalize_sort_key_array_body(values, SortKeyDeclaration::Value(declared), observe)
}

fn normalize_sort_key_array_body<E: From<SortKeyFailure>>(
    values: &ArrayRef,
    declaration: SortKeyDeclaration<'_>,
    observe: &mut impl FnMut(SortKeyConstruction<'_>) -> Result<(), E>,
) -> Result<ArrayRef, E> {
    match values.data_type() {
        DataType::Null => {
            observe(SortKeyConstruction::BufferAdoption)?;
            observe(SortKeyConstruction::ArrayHeader(&DataType::Int8))?;
            return Ok(Arc::new(Int8Array::from(vec![None::<i8>; values.len()])) as ArrayRef);
        }
        DataType::List(field) => {
            let list = values
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| E::from(SortKeyFailure::PhysicalWrapper("ListArray")))?;
            let normalized_values =
                normalize_sort_key_array_body(list.values(), declaration.child(0)?, observe)?;
            if normalized_values.data_type() == list.values().data_type() {
                return Ok(values.clone());
            }
            observe(SortKeyConstruction::Field {
                source: field,
                target: normalized_values.data_type(),
            })?;
            let normalized_field = Arc::new(Field::new(
                field.name(),
                normalized_values.data_type().clone(),
                field.is_nullable(),
            ));
            observe(SortKeyConstruction::ArrayHeader(values.data_type()))?;
            observe(SortKeyConstruction::BufferAdoption)?;
            let normalized = ListArray::new(
                normalized_field,
                OffsetBuffer::new(list.value_offsets().to_vec().into()),
                normalized_values,
                list.nulls().cloned(),
            );
            return Ok(Arc::new(normalized) as ArrayRef);
        }
        DataType::LargeList(field) => {
            let list = values
                .as_any()
                .downcast_ref::<LargeListArray>()
                .ok_or_else(|| E::from(SortKeyFailure::PhysicalWrapper("LargeListArray")))?;
            let normalized_values =
                normalize_sort_key_array_body(list.values(), declaration.child(0)?, observe)?;
            if normalized_values.data_type() == list.values().data_type() {
                return Ok(values.clone());
            }
            observe(SortKeyConstruction::Field {
                source: field,
                target: normalized_values.data_type(),
            })?;
            let normalized_field = Arc::new(Field::new(
                field.name(),
                normalized_values.data_type().clone(),
                field.is_nullable(),
            ));
            observe(SortKeyConstruction::ArrayHeader(values.data_type()))?;
            observe(SortKeyConstruction::BufferAdoption)?;
            return Ok(Arc::new(LargeListArray::new(
                normalized_field,
                OffsetBuffer::new(list.value_offsets().to_vec().into()),
                normalized_values,
                list.nulls().cloned(),
            )) as ArrayRef);
        }
        DataType::FixedSizeList(field, width) => {
            let list = values
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or_else(|| E::from(SortKeyFailure::PhysicalWrapper("FixedSizeListArray")))?;
            let normalized_values =
                normalize_sort_key_array_body(list.values(), declaration.child(0)?, observe)?;
            if normalized_values.data_type() == list.values().data_type() {
                return Ok(values.clone());
            }
            observe(SortKeyConstruction::Field {
                source: field,
                target: normalized_values.data_type(),
            })?;
            let normalized_field = Arc::new(Field::new(
                field.name(),
                normalized_values.data_type().clone(),
                field.is_nullable(),
            ));
            observe(SortKeyConstruction::ArrayHeader(values.data_type()))?;
            return Ok(Arc::new(FixedSizeListArray::new(
                normalized_field,
                *width,
                normalized_values,
                list.nulls().cloned(),
            )) as ArrayRef);
        }
        DataType::Struct(fields) => {
            let struct_arr = values
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| E::from(SortKeyFailure::PhysicalWrapper("StructArray")))?;
            let mut changed = false;
            observe(SortKeyConstruction::ArrayTable(struct_arr.num_columns()))?;
            let mut normalized_columns = Vec::with_capacity(struct_arr.num_columns());
            for (index, column) in struct_arr.columns().iter().enumerate() {
                let normalized =
                    normalize_sort_key_array_body(column, declaration.child(index)?, observe)?;
                changed |= normalized.data_type() != column.data_type();
                normalized_columns.push(normalized);
            }
            if !changed {
                return Ok(values.clone());
            }
            observe(SortKeyConstruction::FieldTable(normalized_columns.len()))?;
            let mut normalized_fields = Vec::with_capacity(normalized_columns.len());
            for (field, column) in fields.iter().zip(&normalized_columns) {
                observe(SortKeyConstruction::Field {
                    source: field,
                    target: column.data_type(),
                })?;
                normalized_fields.push(Arc::new(Field::new(
                    field.name(),
                    column.data_type().clone(),
                    field.is_nullable(),
                )));
            }
            // Conversion to the original shared Fields backing is a separate
            // public constructor from the temporary FieldRef vector.
            observe(SortKeyConstruction::SharedFields(normalized_columns.len()))?;
            let normalized_fields = Fields::from(normalized_fields);
            observe(SortKeyConstruction::ArrayHeader(values.data_type()))?;
            let normalized = StructArray::new(
                normalized_fields,
                normalized_columns,
                struct_arr.nulls().cloned(),
            );
            return Ok(Arc::new(normalized) as ArrayRef);
        }
        DataType::Map(entries_field, ordered) => {
            let map = values
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or_else(|| E::from(SortKeyFailure::PhysicalWrapper("MapArray")))?;
            observe(SortKeyConstruction::ArrayTable(map.entries().num_columns()))?;
            observe(SortKeyConstruction::ArrayHeader(map.entries().data_type()))?;
            let entries = Arc::new(map.entries().clone()) as ArrayRef;
            let normalized_entries =
                normalize_sort_key_array_body(&entries, declaration.map_entries()?, observe)?;
            if normalized_entries.data_type() == entries.data_type() {
                return Ok(values.clone());
            }
            observe(SortKeyConstruction::ArrayTable(map.entries().num_columns()))?;
            let normalized_entries = normalized_entries
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| E::from(SortKeyFailure::PhysicalWrapper("StructArray entries")))?
                .clone();
            observe(SortKeyConstruction::Field {
                source: entries_field,
                target: normalized_entries.data_type(),
            })?;
            let normalized_field = Arc::new(Field::new(
                entries_field.name(),
                DataType::Struct(normalized_entries.fields().clone()),
                entries_field.is_nullable(),
            ));
            observe(SortKeyConstruction::ArrayHeader(values.data_type()))?;
            observe(SortKeyConstruction::BufferAdoption)?;
            let normalized = MapArray::new(
                normalized_field,
                OffsetBuffer::new(map.value_offsets().to_vec().into()),
                normalized_entries,
                map.nulls().cloned(),
                *ordered,
            );
            return Ok(Arc::new(normalized) as ArrayRef);
        }
        _ => {}
    }

    if !declaration.is_largeint(values.data_type()) {
        return Ok(values.clone());
    }
    let array = values
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| E::from(SortKeyFailure::PhysicalWrapper("FixedSizeBinaryArray")))?;
    if array.value_length() != largeint::LARGEINT_BYTE_WIDTH {
        return Err(E::from(SortKeyFailure::LargeIntWidth {
            actual: array.value_length(),
        }));
    }

    let mut decoded = Vec::with_capacity(array.len());
    for row in 0..array.len() {
        observe(SortKeyConstruction::Work(1))?;
        if array.is_null(row) {
            decoded.push(None);
        } else {
            decoded.push(Some(i128::from_be_bytes(
                array
                    .value(row)
                    .try_into()
                    .expect("validated FixedSizeBinary16 value width"),
            )));
        }
    }

    observe(SortKeyConstruction::ArrayHeader(&DataType::Decimal128(
        38, 0,
    )))?;
    observe(SortKeyConstruction::BufferAdoption)?;
    let decimal = Decimal128Array::from(decoded)
        .with_precision_and_scale(38, 0)
        .map_err(|source| E::from(SortKeyFailure::Arrow(source)))?;
    Ok(Arc::new(decimal) as ArrayRef)
}

#[cfg(test)]
pub(crate) fn append_stable_row_index_sort_column(
    sort_columns: &mut Vec<SortColumn>,
    num_rows: usize,
) {
    append_stable_row_index_sort_column_observed(sort_columns, num_rows, &mut |_| Ok(())).unwrap()
}

pub(crate) enum StableIndexConstruction {
    ArrayHeader,
    ColumnGrowth(std::alloc::Layout),
    BufferAdoption,
}

pub(crate) fn append_stable_row_index_sort_column_observed(
    sort_columns: &mut Vec<SortColumn>,
    num_rows: usize,
    observe: &mut impl FnMut(
        StableIndexConstruction,
    ) -> Result<(), crate::exec::pipeline::operator::ConstructionFailure>,
) -> Result<(), crate::exec::pipeline::operator::ConstructionFailure> {
    // The original UInt64 row-id payload remains its existing owner. Only the
    // newly adopted Buffer header and public Array Arc belong to this seam.
    observe(StableIndexConstruction::BufferAdoption)?;
    observe(StableIndexConstruction::ArrayHeader)?;
    let row_ids = UInt64Array::from_iter_values((0..num_rows).map(|idx| idx as u64));
    let growth = novarocks_type_contract::owned_resources::vec::push_growth(sort_columns)
        .map_err(|cause| match cause {
            novarocks_type_contract::ControlResourceError::Control(cause) => crate::exec::pipeline::operator::ConstructionFailure::Control(cause),
            novarocks_type_contract::ControlResourceError::SourceModel(_) => crate::exec::pipeline::operator::ConstructionFailure::Resource(novarocks_type_contract::owned_resources::layout::LayoutResourceError::SourceModel),
        })?;
    if let Some(layout) = growth.requested_backing {
        observe(StableIndexConstruction::ColumnGrowth(layout))?;
    }
    sort_columns.push(SortColumn {
        values: Arc::new(row_ids) as ArrayRef,
        options: Some(SortOptions {
            descending: false,
            nulls_first: true,
        }),
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::chunk::ChunkSchema;
    use arrow::array::{Int32Builder, ListBuilder, StringArray};
    use arrow::compute::lexsort_to_indices;
    use novarocks_types::SlotId;

    fn decimal_chunk(precision: u8, scale: i8, value: i128) -> Chunk {
        let data_type = DataType::Decimal128(precision, scale);
        let array = Decimal128Array::from(vec![Some(value)])
            .with_precision_and_scale(precision, scale)
            .expect("decimal array");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "price",
            data_type.clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array) as ArrayRef])
            .expect("record batch");
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &[SlotId::new(1)])
                .expect("chunk schema");
        Chunk::try_new_with_chunk_schema(batch, chunk_schema).expect("chunk")
    }

    #[test]
    fn merged_sort_schema_rejects_decimal_precision_drift() {
        let left = decimal_chunk(10, 2, 1234);
        let right = decimal_chunk(38, 2, 5678);

        let err = merged_sort_schema_for_chunks(&[left, right])
            .expect_err("sort concat must reject decimal precision drift");

        assert!(err.contains("sort schema field mismatch"), "err={err}");
        assert!(err.contains("Decimal128(10, 2)"), "err={err}");
        assert!(err.contains("Decimal128(38, 2)"), "err={err}");
    }

    #[test]
    fn normalize_sort_batch_rejects_utf8_binary_type_drift() {
        let source_schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Utf8,
            true,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&source_schema),
            vec![Arc::new(StringArray::from(vec![Some("abc")])) as ArrayRef],
        )
        .expect("record batch");
        let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            source_schema.as_ref(),
            &[SlotId::new(7)],
        )
        .expect("chunk schema");
        let chunk = Chunk::try_new_with_chunk_schema(batch, chunk_schema).expect("chunk");
        let target_schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Binary,
            true,
        )]));

        let err = normalize_sort_batch_for_schema(&chunk, &target_schema, 0)
            .expect_err("sort normalize must reject Utf8/Binary descriptor drift");

        assert!(err.contains("sort payload type mismatch"), "err={err}");
        assert!(err.contains("Utf8"), "err={err}");
        assert!(err.contains("Binary"), "err={err}");
    }

    fn append_inner_list(
        builder: &mut ListBuilder<ListBuilder<Int32Builder>>,
        values: &[Option<i32>],
    ) {
        for value in values {
            builder.values().values().append_option(*value);
        }
        builder.values().append(true);
    }

    #[test]
    fn complex_sort_keeps_parent_nulls_first_and_inner_nulls_first() {
        let mut builder = ListBuilder::new(ListBuilder::new(Int32Builder::new()));

        builder.append(false);

        append_inner_list(&mut builder, &[Some(1)]);
        builder.values().append(false);
        append_inner_list(&mut builder, &[Some(2)]);
        builder.append(true);

        append_inner_list(&mut builder, &[Some(1)]);
        append_inner_list(&mut builder, &[Some(2)]);
        builder.append(true);

        let values = Arc::new(builder.finish()) as ArrayRef;
        let values = normalize_sort_key_array(&values).expect("normalize sort key");
        let mut sort_columns = vec![SortColumn {
            values,
            options: Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
        }];
        append_stable_row_index_sort_column(&mut sort_columns, 3);

        let indices = lexsort_to_indices(&sort_columns, None).expect("sort indices");
        let actual = (0..indices.len())
            .map(|idx| indices.value(idx))
            .collect::<Vec<_>>();

        assert_eq!(actual, vec![0, 1, 2]);
    }
    fn fixed16_values() -> ArrayRef {
        Arc::new(
            FixedSizeBinaryArray::try_from_iter([-1i128, 0, 1].into_iter().map(i128::to_be_bytes))
                .unwrap(),
        )
    }
    fn semantic(kind: novarocks_type_contract::ValueKind) -> novarocks_type_contract::ValueTypeRef {
        novarocks_type_contract::ValueTypeRef::try_new(
            kind,
            false,
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
        )
        .unwrap()
    }
    fn normalize_declared(
        values: &ArrayRef,
        ty: &novarocks_type_contract::ValueTypeRef,
    ) -> ArrayRef {
        normalize_sort_key_array_observed::<SortKeyFailure>(values, Some(ty), &mut |_| Ok(()))
            .unwrap()
    }
    fn actual_sort_order(values: ArrayRef) -> Vec<u32> {
        let indices = lexsort_to_indices(
            &[SortColumn {
                values,
                options: Some(SortOptions {
                    descending: false,
                    nulls_first: true,
                }),
            }],
            None,
        )
        .unwrap();
        indices.values().to_vec()
    }
    #[test]
    fn typed_sort_fixed16_plain_and_uuid_remain_bytes_while_largeint_is_signed() {
        use novarocks_type_contract::{ValueKind, ValueLogicalType};
        let array = fixed16_values();
        for kind in [
            ValueKind::Scalar(DataType::FixedSizeBinary(16)),
            ValueKind::Nominal(ValueLogicalType::Uuid),
        ] {
            let declared = semantic(kind);
            let normalized = normalize_declared(&array, &declared);
            assert!(Arc::ptr_eq(&normalized, &array));
            assert_eq!(actual_sort_order(normalized), vec![1, 2, 0]);
        }
        let largeint = semantic(ValueKind::Nominal(ValueLogicalType::LargeInt));
        let normalized = normalize_declared(&array, &largeint);
        assert_eq!(normalized.data_type(), &DataType::Decimal128(38, 0));
        assert_eq!(actual_sort_order(normalized), vec![0, 1, 2]);
    }
    #[test]
    fn typed_sort_nested_struct_list_and_map_borrow_actual_semantic_children() {
        use novarocks_type_contract::{
            ValueField, ValueKind, ValueLogicalType, ValueTypeStorageLifetime,
        };
        let storage = ValueTypeStorageLifetime::pure();
        let largeint = semantic(ValueKind::Nominal(ValueLogicalType::LargeInt));
        let uuid = semantic(ValueKind::Nominal(ValueLogicalType::Uuid));
        let plain = semantic(ValueKind::Scalar(DataType::FixedSizeBinary(16)));
        let array = fixed16_values();
        let fields = Fields::from(vec![
            Arc::new(Field::new("signed", DataType::FixedSizeBinary(16), false)),
            Arc::new(Field::new("uuid", DataType::FixedSizeBinary(16), false)),
            Arc::new(Field::new("bytes", DataType::FixedSizeBinary(16), false)),
        ]);
        let structure = Arc::new(StructArray::new(
            fields,
            vec![array.clone(), array.clone(), array.clone()],
            None,
        )) as ArrayRef;
        let declared = semantic(ValueKind::Struct(
            vec![
                ValueField::new("signed", largeint.clone(), storage.clone()),
                ValueField::new("uuid", uuid.clone(), storage.clone()),
                ValueField::new("bytes", plain, storage),
            ]
            .into_boxed_slice(),
        ));
        let normalized = normalize_declared(&structure, &declared);
        let result = normalized.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(result.column(0).data_type(), &DataType::Decimal128(38, 0));
        assert!(Arc::ptr_eq(result.column(1), &array));
        assert!(Arc::ptr_eq(result.column(2), &array));
        assert!(
            result
                .fields()
                .iter()
                .all(|field| field.metadata().as_arc().is_none())
        );
        assert_eq!(actual_sort_order(normalized), vec![0, 1, 2]);
        for (child, order) in [(largeint.clone(), vec![0, 1, 2]), (uuid, vec![1, 2, 0])] {
            let values = Arc::new(ListArray::new(
                Arc::new(Field::new("item", DataType::FixedSizeBinary(16), false)),
                OffsetBuffer::new(vec![0i32, 1, 2, 3].into()),
                array.clone(),
                None,
            )) as ArrayRef;
            let declared = semantic(ValueKind::List(child));
            assert_eq!(
                actual_sort_order(normalize_declared(&values, &declared)),
                order
            );
        }
        let entries = StructArray::new(
            Fields::from(vec![
                Arc::new(Field::new("key", DataType::Utf8, false)),
                Arc::new(Field::new("value", DataType::FixedSizeBinary(16), false)),
            ]),
            vec![Arc::new(StringArray::from(vec!["a", "a", "a"])), array],
            None,
        );
        let map = Arc::new(MapArray::new(
            Arc::new(Field::new("entries", entries.data_type().clone(), false)),
            OffsetBuffer::new(vec![0i32, 1, 2, 3].into()),
            entries,
            None,
            false,
        )) as ArrayRef;
        let declared = semantic(ValueKind::Map {
            key: semantic(ValueKind::String),
            value: largeint,
        });
        let normalized = normalize_declared(&map, &declared);
        let result = normalized.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(
            result.entries().column(1).data_type(),
            &DataType::Decimal128(38, 0)
        );
        assert_eq!(
            actual_sort_order(result.entries().column(1).clone()),
            vec![0, 1, 2]
        );
        assert_eq!(result.value_offsets(), &[0, 1, 2, 3]);
    }
    #[test]
    fn typed_sort_missing_declaration_and_original_observer_refusal_have_no_tail() {
        use novarocks_type_contract::{ValueKind, ValueLogicalType};
        #[derive(Debug)]
        enum Failure {
            Declaration(SortKeyFailure),
            Caller(Arc<u32>),
        }
        impl From<SortKeyFailure> for Failure {
            fn from(value: SortKeyFailure) -> Self {
                Self::Declaration(value)
            }
        }
        let values = fixed16_values();
        let mut seen = 0;
        let missing = normalize_sort_key_array_observed::<Failure>(&values, None, &mut |_| {
            seen += 1;
            Ok(())
        });
        assert!(matches!(
            missing,
            Err(Failure::Declaration(SortKeyFailure::MissingDeclaration))
        ));
        assert_eq!(seen, 0);
        let declared = semantic(ValueKind::Nominal(ValueLogicalType::LargeInt));
        let mut total = 0;
        normalize_sort_key_array_observed::<Failure>(&values, Some(&declared), &mut |_| {
            total += 1;
            Ok(())
        })
        .unwrap();
        assert!(total > 0);
        let cause = Arc::new(17);
        for refused in 0..total {
            let mut seen = 0;
            let result =
                normalize_sort_key_array_observed::<Failure>(&values, Some(&declared), &mut |_| {
                    let boundary = seen;
                    seen += 1;
                    if boundary == refused {
                        Err(Failure::Caller(cause.clone()))
                    } else {
                        Ok(())
                    }
                });
            match result {
                Err(Failure::Caller(original)) => assert!(Arc::ptr_eq(&original, &cause)),
                other => panic!("wrong caller cause: {other:?}"),
            }
            assert_eq!(seen, refused + 1);
        }
    }
    #[test]
    fn typed_sort_large_and_fixed_lists_keep_uuid_bytes_and_normalize_declared_largeint() {
        use novarocks_type_contract::{ValueKind, ValueLogicalType};
        for (domain, expected) in [
            (ValueLogicalType::LargeInt, vec![0, 1, 2]),
            (ValueLogicalType::Uuid, vec![1, 2, 0]),
        ] {
            let child = semantic(ValueKind::Nominal(domain));
            let array = fixed16_values();
            let field = Arc::new(Field::new("item", DataType::FixedSizeBinary(16), false));
            let large = Arc::new(LargeListArray::new(
                field.clone(),
                OffsetBuffer::new(vec![0i64, 1, 2, 3].into()),
                array.clone(),
                None,
            )) as ArrayRef;
            let large_type = semantic(ValueKind::List(child.clone()));
            assert_eq!(
                actual_sort_order(normalize_declared(&large, &large_type)),
                expected
            );
            let fixed = Arc::new(FixedSizeListArray::new(field, 1, array, None)) as ArrayRef;
            let fixed_type = semantic(ValueKind::FixedSizeList {
                item: child,
                length: 1,
            });
            assert_eq!(
                actual_sort_order(normalize_declared(&fixed, &fixed_type)),
                expected
            );
        }
    }
}
