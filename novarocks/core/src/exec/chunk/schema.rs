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
use std::collections::HashMap;
use std::sync::Arc;

use crate::common::ids::SlotId;
use crate::exec::chunk::type_compatibility::{check_exact, nested_path_label};
use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use novarocks_types::logical::{LogicalType, logical_type_of_field};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ChunkFieldSchema {
    logical_type: Option<LogicalType>,
    children: Vec<ChunkFieldSchema>,
}

impl ChunkFieldSchema {
    #[cfg(test)]
    pub(crate) fn empty() -> Self {
        Self {
            logical_type: None,
            children: Vec::new(),
        }
    }

    pub fn from_field(field: &Field) -> Result<Self, String> {
        Ok(Self {
            logical_type: logical_type_of_field(field),
            children: Self::children_from_arrow_data_type(field.data_type())?,
        })
    }

    fn children_from_arrow_data_type(data_type: &DataType) -> Result<Vec<Self>, String> {
        match data_type {
            DataType::Struct(fields) => fields
                .iter()
                .map(|child| Self::from_field(child.as_ref()))
                .collect::<Result<Vec<_>, _>>(),
            DataType::List(item) | DataType::LargeList(item) => {
                Ok(vec![Self::from_field(item.as_ref())?])
            }
            DataType::Map(entries, _) => {
                let DataType::Struct(entry_fields) = entries.data_type() else {
                    return Err(format!(
                        "map entries is not struct: {:?}",
                        entries.data_type()
                    ));
                };
                if entry_fields.len() != 2 {
                    return Err(format!(
                        "map entries expected 2 struct fields, got {}",
                        entry_fields.len()
                    ));
                }
                Ok(vec![
                    Self::from_field(entry_fields[0].as_ref())?,
                    Self::from_field(entry_fields[1].as_ref())?,
                ])
            }
            _ => Ok(Vec::new()),
        }
    }

    pub fn logical_type(&self) -> Option<LogicalType> {
        self.logical_type
    }

    pub fn json_semantic(&self) -> bool {
        self.logical_type == Some(LogicalType::Json)
    }

    pub fn children(&self) -> &[ChunkFieldSchema] {
        &self.children
    }

    pub fn struct_child(&self, idx: usize) -> Option<&ChunkFieldSchema> {
        self.children.get(idx)
    }

    pub fn list_item(&self) -> Option<&ChunkFieldSchema> {
        self.children.first()
    }

    pub fn map_key(&self) -> Option<&ChunkFieldSchema> {
        self.children.first()
    }

    pub fn map_value(&self) -> Option<&ChunkFieldSchema> {
        self.children.get(1)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ChunkSlotSchema {
    slot_id: SlotId,
    field: Field,
    field_schema: ChunkFieldSchema,
    unique_id: Option<i32>,
}

impl ChunkSlotSchema {
    pub fn new_with_field(
        slot_id: SlotId,
        field: Field,
        field_schema: Option<ChunkFieldSchema>,
        unique_id: Option<i32>,
    ) -> Self {
        Self::try_new_with_field(slot_id, field, field_schema, unique_id)
            .unwrap_or_else(|e| panic!("{e}"))
    }

    pub fn try_new_with_field(
        slot_id: SlotId,
        field: Field,
        field_schema: Option<ChunkFieldSchema>,
        unique_id: Option<i32>,
    ) -> Result<Self, String> {
        Ok(Self {
            slot_id,
            field_schema: match field_schema {
                Some(schema) => schema,
                None => ChunkFieldSchema::from_field(&field)?,
            },
            field,
            unique_id,
        })
    }

    /// Return a copy of this slot schema with nullable set to the given value.
    pub fn with_nullable(&self, nullable: bool) -> Self {
        if self.field.is_nullable() == nullable {
            return self.clone();
        }
        Self {
            slot_id: self.slot_id,
            field: self.field.clone().with_nullable(nullable),
            field_schema: self.field_schema.clone(),
            unique_id: self.unique_id,
        }
    }

    pub fn from_field(
        slot_id: SlotId,
        field: &Field,
        unique_id: Option<i32>,
    ) -> Result<Self, String> {
        Self::try_new_with_field(slot_id, field.clone(), None, unique_id)
    }

    pub fn with_field(&self, field: Field) -> Result<Self, String> {
        Self::try_new_with_field(
            self.slot_id,
            field,
            Some(self.field_schema.clone()),
            self.unique_id,
        )
    }

    pub fn with_slot_id(&self, slot_id: SlotId) -> Result<Self, String> {
        Self::try_new_with_field(
            slot_id,
            self.field.clone(),
            Some(self.field_schema.clone()),
            self.unique_id,
        )
    }

    pub fn with_field_and_slot_id(&self, slot_id: SlotId, field: Field) -> Result<Self, String> {
        Self::try_new_with_field(
            slot_id,
            field,
            Some(self.field_schema.clone()),
            self.unique_id,
        )
    }

    pub fn slot_id(&self) -> SlotId {
        self.slot_id
    }

    pub fn field(&self) -> &Field {
        &self.field
    }

    pub fn name(&self) -> &str {
        self.field.name()
    }

    pub fn nullable(&self) -> bool {
        self.field.is_nullable()
    }

    pub fn data_type(&self) -> &DataType {
        self.field.data_type()
    }

    pub fn unique_id(&self) -> Option<i32> {
        self.unique_id
    }

    pub fn field_schema(&self) -> &ChunkFieldSchema {
        &self.field_schema
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ChunkSchema {
    slots: Vec<ChunkSlotSchema>,
    arrow_schema: SchemaRef,
    slot_ids: Vec<SlotId>,
    index_by_slot: HashMap<SlotId, usize>,
}

pub type ChunkSchemaRef = Arc<ChunkSchema>;

fn check_chunk_data_type(
    expected: &DataType,
    actual: &DataType,
    root_label: &str,
) -> Result<(), String> {
    check_exact(expected, actual).map_err(|m| {
        format!(
            "chunk schema type mismatch at {}: expected {:?}, got {:?} ({:?})",
            nested_path_label(root_label, &m.nested_path),
            expected,
            actual,
            m.kind
        )
    })
}

fn reconcile_chunk_field_to_field(expected: &Field, actual: &Field) -> Result<Arc<Field>, String> {
    let data_type = reconcile_chunk_data_type(expected.data_type(), actual.data_type())?;
    let nullable = expected.is_nullable() || actual.is_nullable();
    if &data_type == expected.data_type() && nullable == expected.is_nullable() {
        Ok(Arc::new(expected.clone()))
    } else {
        Ok(Arc::new(rebuild_chunk_field(expected, data_type, nullable)))
    }
}

fn reconcile_chunk_field_to_data_type(
    expected: &Field,
    actual: &DataType,
    actual_nullable: bool,
) -> Result<Arc<Field>, String> {
    let data_type = reconcile_chunk_data_type(expected.data_type(), actual)?;
    let nullable = expected.is_nullable() || actual_nullable;
    if &data_type == expected.data_type() && nullable == expected.is_nullable() {
        Ok(Arc::new(expected.clone()))
    } else {
        Ok(Arc::new(rebuild_chunk_field(expected, data_type, nullable)))
    }
}

fn rebuild_chunk_field(expected: &Field, data_type: DataType, nullable: bool) -> Field {
    Field::new(expected.name(), data_type, nullable).with_metadata(expected.metadata().clone())
}

fn reconcile_chunk_data_type(expected: &DataType, actual: &DataType) -> Result<DataType, String> {
    if expected == actual {
        return Ok(expected.clone());
    }
    check_chunk_data_type(expected, actual, "column")?;
    if is_dictionary_string_carrier(expected, actual) {
        return Ok(actual.clone());
    }
    Ok(expected.clone())
}

fn is_dictionary_string_carrier(expected: &DataType, actual: &DataType) -> bool {
    matches!(
        (expected, actual),
        (
            DataType::Utf8 | DataType::LargeUtf8,
            DataType::Dictionary(key, value),
        ) if key.as_ref() == &DataType::Int32 && value.as_ref() == expected
    )
}

impl ChunkSchema {
    pub fn try_new(slots: Vec<ChunkSlotSchema>) -> Result<Self, String> {
        Self::try_new_with_schema_metadata(slots, HashMap::new())
    }

    pub fn try_new_with_schema_metadata(
        slots: Vec<ChunkSlotSchema>,
        metadata: HashMap<String, String>,
    ) -> Result<Self, String> {
        let mut index_by_slot = HashMap::with_capacity(slots.len());
        let mut slot_ids = Vec::with_capacity(slots.len());
        let mut fields = Vec::with_capacity(slots.len());
        for (idx, slot) in slots.iter().enumerate() {
            if index_by_slot.insert(slot.slot_id(), idx).is_some() {
                return Err(format!(
                    "duplicate slot id {} in chunk schema contract at index {}",
                    slot.slot_id(),
                    idx
                ));
            }
            slot_ids.push(slot.slot_id());
            fields.push(Arc::new(slot.field().clone()));
        }
        Ok(Self {
            slots,
            arrow_schema: Arc::new(arrow::datatypes::Schema::new_with_metadata(
                fields, metadata,
            )),
            slot_ids,
            index_by_slot,
        })
    }

    pub fn empty() -> Self {
        Self {
            slots: Vec::new(),
            arrow_schema: Arc::new(arrow::datatypes::Schema::empty()),
            slot_ids: Vec::new(),
            index_by_slot: HashMap::new(),
        }
    }

    pub fn slots(&self) -> &[ChunkSlotSchema] {
        &self.slots
    }

    pub fn arrow_schema_ref(&self) -> SchemaRef {
        Arc::clone(&self.arrow_schema)
    }

    pub fn slot_ids(&self) -> &[SlotId] {
        &self.slot_ids
    }

    pub fn field(&self, idx: usize) -> Option<&Field> {
        self.slots.get(idx).map(ChunkSlotSchema::field)
    }

    pub fn field_by_slot(&self, slot_id: SlotId) -> Option<&Field> {
        self.slot(slot_id).map(ChunkSlotSchema::field)
    }

    pub fn slot_schema_from_arrow_field(
        slot_id: SlotId,
        field: &Field,
    ) -> Result<ChunkSlotSchema, String> {
        ChunkSlotSchema::from_field(slot_id, field, None)
    }

    pub fn try_ref_from_schema_and_slot_ids(
        schema: &Schema,
        slot_ids: &[SlotId],
    ) -> Result<ChunkSchemaRef, String> {
        if schema.fields().len() != slot_ids.len() {
            return Err(format!(
                "chunk schema slot id length mismatch: schema_fields={} slot_ids={}",
                schema.fields().len(),
                slot_ids.len()
            ));
        }
        let slots = schema
            .fields()
            .iter()
            .zip(slot_ids.iter().copied())
            .map(|(field, slot_id)| Self::slot_schema_from_arrow_field(slot_id, field.as_ref()))
            .collect::<Result<Vec<_>, _>>()?;
        Self::try_new(slots).map(Arc::new)
    }

    pub fn slot(&self, slot_id: SlotId) -> Option<&ChunkSlotSchema> {
        self.index_by_slot
            .get(&slot_id)
            .and_then(|idx| self.slots.get(*idx))
    }

    pub fn index_of(&self, slot_id: SlotId) -> Option<usize> {
        self.index_by_slot.get(&slot_id).copied()
    }

    pub fn index_by_slot(&self) -> &HashMap<SlotId, usize> {
        &self.index_by_slot
    }

    pub fn project_by_slots(&self, slot_ids: &[SlotId]) -> Result<Self, String> {
        let mut slots = Vec::with_capacity(slot_ids.len());
        for slot_id in slot_ids {
            let slot = self.slot(*slot_id).cloned().ok_or_else(|| {
                format!(
                    "chunk schema projection references missing slot {} (available={:?})",
                    slot_id,
                    self.slot_ids()
                )
            })?;
            slots.push(slot);
        }
        Self::try_new(slots)
    }

    pub fn with_fields_in_order(&self, fields: Vec<Field>) -> Result<Self, String> {
        if fields.len() != self.slots.len() {
            return Err(format!(
                "chunk schema field length mismatch: fields={} slots={}",
                fields.len(),
                self.slots.len()
            ));
        }
        let slots = self
            .slots
            .iter()
            .cloned()
            .zip(fields.into_iter())
            .map(|(slot, field)| slot.with_field(field))
            .collect::<Result<Vec<_>, _>>()?;
        Self::try_new(slots)
    }

    pub fn concat(parts: &[ChunkSchemaRef]) -> Result<Self, String> {
        let mut slots = Vec::new();
        for part in parts {
            slots.extend_from_slice(part.slots());
        }
        Self::try_new(slots)
    }
}

pub(super) fn align_chunk_schema_to_batch(
    batch: &RecordBatch,
    chunk_schema: &ChunkSchema,
) -> Result<ChunkSchemaRef, String> {
    if batch.num_columns() != chunk_schema.slots().len() {
        return Err(format!(
            "chunk schema contract length mismatch: batch_columns={} contract_slots={}",
            batch.num_columns(),
            chunk_schema.slots().len()
        ));
    }
    let mut slots = Vec::with_capacity(batch.num_columns());
    for (idx, field) in batch.schema().fields().iter().enumerate() {
        let expected = chunk_schema
            .slots()
            .get(idx)
            .ok_or_else(|| format!("missing chunk schema slot at index {}", idx))?;
        // Arrow field nullability is producer metadata here. A nullable batch
        // can flow through a non-nullable contract because source-level NOT
        // NULL enforcement happens downstream, and a non-nullable batch is a
        // valid runtime instance of a nullable contract.
        if field.name() != expected.name() {
            return Err(format!(
                "chunk schema field mismatch at index {}: batch=({}, {:?}, {}) contract=({}, {:?}, {})",
                idx,
                field.name(),
                field.data_type(),
                field.is_nullable(),
                expected.name(),
                expected.data_type(),
                expected.nullable()
            ));
        }
        let root = format!("slot {} ({})", expected.slot_id(), expected.name());
        check_chunk_data_type(expected.data_type(), field.data_type(), &root)?;
        let reconciled_field = reconcile_chunk_field_to_field(expected.field(), field.as_ref())?;
        slots.push(
            expected
                .with_field_and_slot_id(expected.slot_id(), reconciled_field.as_ref().clone())?,
        );
    }
    Ok(Arc::new(ChunkSchema::try_new(slots)?))
}

pub(super) fn align_chunk_schema_to_columns(
    columns: &[ArrayRef],
    chunk_schema: &ChunkSchema,
) -> Result<ChunkSchemaRef, String> {
    if columns.len() != chunk_schema.slots().len() {
        return Err(format!(
            "chunk schema contract length mismatch: columns={} contract_slots={}",
            columns.len(),
            chunk_schema.slots().len()
        ));
    }
    let mut slots = Vec::with_capacity(columns.len());
    for (idx, column) in columns.iter().enumerate() {
        let expected = chunk_schema
            .slots()
            .get(idx)
            .ok_or_else(|| format!("missing chunk schema slot at index {}", idx))?;
        let root = format!("slot {} ({})", expected.slot_id(), expected.name());
        check_chunk_data_type(expected.data_type(), column.data_type(), &root)?;
        let reconciled_field = reconcile_chunk_field_to_data_type(
            expected.field(),
            column.data_type(),
            column.null_count() > 0,
        )?;
        slots.push(
            expected
                .with_field_and_slot_id(expected.slot_id(), reconciled_field.as_ref().clone())?,
        );
    }
    Ok(Arc::new(ChunkSchema::try_new(slots)?))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        Array, ArrayRef, BinaryArray, Decimal128Array, Int8Array, Int32Array, Int64Array, MapArray,
        StringArray, StructArray, TimestampMicrosecondArray,
    };
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;

    use super::{ChunkSchema, ChunkSlotSchema};
    use crate::common::ids::SlotId;
    use crate::exec::chunk::Chunk;
    use novarocks_types::logical::{LogicalType, field_with_logical_type, logical_type_of_field};

    #[test]
    fn strict_rejects_duplicate_slot_id() {
        let err = ChunkSchema::try_new(vec![
            ChunkSlotSchema::new_with_field(
                SlotId::new(1),
                Field::new("a", DataType::Int32, true),
                None,
                None,
            ),
            ChunkSlotSchema::new_with_field(
                SlotId::new(1),
                Field::new("b", DataType::Int32, true),
                None,
                None,
            ),
        ])
        .expect_err("duplicate slot ids should fail");
        assert!(err.contains("duplicate slot id"), "err={}", err);
    }

    #[test]
    fn chunk_schema_recovers_logical_metadata_and_unique_id() {
        let hll_field =
            field_with_logical_type(Field::new("a", DataType::Binary, true), LogicalType::Hll);
        let schema = Arc::new(Schema::new(vec![hll_field.clone()]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(BinaryArray::from(vec![Some(b"x".as_slice())]))],
        )
        .expect("record batch");
        let chunk = Chunk::try_new_with_chunk_schema(
            batch,
            Arc::new(
                ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                    SlotId::new(7),
                    hll_field,
                    None,
                    Some(77),
                )])
                .expect("chunk schema"),
            ),
        )
        .expect("chunk");
        let slot = chunk
            .chunk_schema()
            .slot(SlotId::new(7))
            .expect("slot schema");
        assert_eq!(slot.data_type(), &DataType::Binary);
        assert_eq!(slot.field_schema().logical_type(), Some(LogicalType::Hll));
        assert_eq!(logical_type_of_field(slot.field()), Some(LogicalType::Hll));
        assert_eq!(slot.name(), "a");
        assert_eq!(slot.unique_id(), Some(77));
    }

    #[test]
    fn align_chunk_schema_preserves_logical_metadata_when_widening_nullable() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "payload",
                DataType::Utf8,
                true,
            )])),
            vec![Arc::new(StringArray::from(vec![Some(r#"{"a":1}"#)])) as ArrayRef],
        )
        .expect("record batch");
        let contract = Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                SlotId::new(9),
                field_with_logical_type(
                    Field::new("payload", DataType::Utf8, false),
                    LogicalType::Json,
                ),
                None,
                None,
            )])
            .expect("chunk schema"),
        );

        let chunk = Chunk::try_new_with_chunk_schema(batch, contract).expect("chunk");
        let slot = &chunk.chunk_schema().slots()[0];

        assert!(slot.nullable());
        assert_eq!(logical_type_of_field(slot.field()), Some(LogicalType::Json));
    }

    #[test]
    fn reconcile_chunk_field_to_data_type_preserves_logical_metadata_when_widening_nullable() {
        let expected = field_with_logical_type(
            Field::new("payload", DataType::Binary, false),
            LogicalType::Hll,
        );

        let reconciled =
            super::reconcile_chunk_field_to_data_type(&expected, &DataType::Binary, true)
                .expect("reconcile field");

        assert!(reconciled.is_nullable());
        assert_eq!(
            logical_type_of_field(reconciled.as_ref()),
            Some(LogicalType::Hll)
        );
    }

    #[test]
    fn try_new_with_chunk_schema_preserves_zero_column_row_count() {
        let options = arrow::array::RecordBatchOptions::new().with_row_count(Some(3));
        let batch = RecordBatch::try_new_with_options(Arc::new(Schema::empty()), vec![], &options)
            .expect("zero-column record batch");
        let chunk_schema = Arc::new(ChunkSchema::try_new(vec![]).expect("chunk schema"));

        let chunk = Chunk::try_new_with_chunk_schema(batch, chunk_schema).expect("chunk");

        assert_eq!(chunk.batch.num_columns(), 0);
        assert_eq!(chunk.batch.num_rows(), 3);
    }

    #[test]
    fn try_new_with_chunk_schema_reuses_exact_schema_batch_arrays() {
        let column = Arc::new(Int32Array::from(vec![1_i32, 2, 3])) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![Field::new("c1", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::clone(&column)]).expect("batch");
        let chunk_schema = Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                SlotId::new(1),
                Field::new("c1", DataType::Int32, false),
                None,
                None,
            )])
            .expect("chunk schema"),
        );

        let chunk = Chunk::try_new_with_chunk_schema(batch, chunk_schema).expect("chunk");

        assert!(Arc::ptr_eq(&chunk.columns()[0], &column));
    }

    #[test]
    fn align_chunk_schema_accepts_non_nullable_batch_for_nullable_contract() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("c13", DataType::Int8, false)])),
            vec![Arc::new(Int8Array::from(vec![1_i8, 2, 3])) as ArrayRef],
        )
        .expect("record batch");
        let contract = Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                SlotId::new(13),
                Field::new("c13", DataType::Int8, true),
                None,
                None,
            )])
            .expect("chunk schema"),
        );

        let chunk = Chunk::try_new_with_chunk_schema(batch, contract).expect("chunk");

        assert!(
            chunk.chunk_schema().slots()[0].nullable(),
            "aligned chunk schema should keep the descriptor nullability contract"
        );
    }

    #[test]
    fn align_chunk_schema_to_columns_keeps_descriptor_map_key_nullability() {
        let expected_map = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Arc::new(Field::new("key", DataType::Int32, false)),
                        Arc::new(Field::new("value", DataType::Int64, true)),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
        let schema = ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
            SlotId::new(1),
            Field::new("m", expected_map, false),
            None,
            None,
        )])
        .expect("chunk schema");

        let key_array = Arc::new(Int32Array::from(vec![Some(1), None])) as ArrayRef;
        let value_array = Arc::new(Int64Array::from(vec![Some(10), Some(20)])) as ArrayRef;
        let entries = StructArray::new(
            Fields::from(vec![
                Arc::new(Field::new("key", DataType::Int32, true)),
                Arc::new(Field::new("value", DataType::Int64, true)),
            ]),
            vec![key_array, value_array],
            None,
        );
        let map = Arc::new(MapArray::new(
            Arc::new(Field::new("entries", entries.data_type().clone(), false)),
            OffsetBuffer::new(vec![0, 2].into()),
            entries,
            None,
            false,
        )) as ArrayRef;

        let aligned = super::align_chunk_schema_to_columns(&[map], &schema).expect("align schema");
        let DataType::Map(entries_field, _) = aligned.slots()[0].data_type() else {
            panic!("expected map type");
        };
        let DataType::Struct(entry_fields) = entries_field.data_type() else {
            panic!("expected entry struct");
        };
        assert!(
            !entry_fields[0].is_nullable(),
            "map key should keep descriptor nullability"
        );
    }

    #[test]
    fn align_chunk_schema_to_columns_rejects_utf8_binary_type_drift() {
        let schema = ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
            SlotId::new(9),
            Field::new("payload", DataType::Binary, true),
            None,
            None,
        )])
        .expect("chunk schema");
        let column = Arc::new(arrow::array::StringArray::from(vec![Some("abc")])) as ArrayRef;

        let err = super::align_chunk_schema_to_columns(&[column], &schema)
            .expect_err("runtime schema must reject Utf8/Binary descriptor drift");
        assert!(err.contains("chunk schema type mismatch"), "err={err}");
        assert!(err.contains("slot 9 (payload)"), "err={err}");
    }

    #[test]
    fn try_new_with_columns_rejects_utf8_binary_type_drift() {
        let chunk_schema = Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                SlotId::new(9),
                Field::new("payload", DataType::Binary, true),
                None,
                None,
            )])
            .expect("chunk schema"),
        );
        let column = Arc::new(StringArray::from(vec![Some("abc"), Some("xyz")])) as ArrayRef;

        let err = Chunk::try_new_with_columns(chunk_schema, vec![column])
            .expect_err("runtime schema must reject Utf8/Binary descriptor drift");
        assert!(err.contains("chunk schema type mismatch"), "err={err}");
        assert!(err.contains("slot 9 (payload)"), "err={err}");
    }

    #[test]
    fn try_new_with_chunk_schema_rejects_utf8_binary_type_drift() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "payload",
                DataType::Utf8,
                true,
            )])),
            vec![Arc::new(StringArray::from(vec![Some("abc"), Some("xyz")]))],
        )
        .expect("record batch");
        let chunk_schema = Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                SlotId::new(9),
                Field::new("payload", DataType::Binary, true),
                None,
                None,
            )])
            .expect("chunk schema"),
        );

        let err = Chunk::try_new_with_chunk_schema(batch, chunk_schema)
            .expect_err("runtime schema must reject Utf8/Binary descriptor drift");

        assert!(err.contains("chunk schema type mismatch"), "err={err}");
        assert!(err.contains("slot 9 (payload)"), "err={err}");
        assert!(err.contains("Binary"), "err={err}");
        assert!(err.contains("Utf8"), "err={err}");
    }

    #[test]
    fn try_new_with_chunk_schema_rejects_same_scale_decimal_precision_drift() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "price",
                DataType::Decimal128(10, 2),
                true,
            )])),
            vec![Arc::new(
                Decimal128Array::from(vec![Some(1234_i128)])
                    .with_precision_and_scale(10, 2)
                    .expect("decimal array"),
            ) as ArrayRef],
        )
        .expect("record batch");
        let chunk_schema = Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                SlotId::new(11),
                Field::new("price", DataType::Decimal128(38, 2), true),
                None,
                None,
            )])
            .expect("chunk schema"),
        );

        let err = Chunk::try_new_with_chunk_schema(batch, chunk_schema)
            .expect_err("runtime schema must reject decimal precision drift");

        assert!(err.contains("chunk schema type mismatch"), "err={err}");
        assert!(err.contains("slot 11 (price)"), "err={err}");
        assert!(err.contains("Decimal128(38, 2)"), "err={err}");
        assert!(err.contains("Decimal128(10, 2)"), "err={err}");
    }

    #[test]
    fn try_new_with_chunk_schema_rejects_timestamp_metadata_retag() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            )])),
            vec![Arc::new(TimestampMicrosecondArray::from(vec![
                Some(1_000_i64),
                Some(2_000),
            ]))],
        )
        .expect("record batch");
        let chunk_schema = Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                SlotId::new(10),
                Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
                None,
                None,
            )])
            .expect("chunk schema"),
        );

        let err = Chunk::try_new_with_chunk_schema(batch, chunk_schema)
            .expect_err("timestamp metadata retag should fail");

        assert!(err.contains("slot 10 (ts)"), "err={err}");
        assert!(err.contains("Timestamp(Nanosecond, None)"), "err={err}");
    }
}
