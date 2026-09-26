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

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use arrow_schema::{Schema, SchemaRef};
use novarocks_types::SlotId;
use novarocks_types::logical::LogicalType;
use sha2::{Digest, Sha256};

use crate::{LayoutIdentity, StaticFieldSchema};

const MAX_SLOT_METADATA_DEPTH: usize = 64;
const MAX_SLOT_METADATA_NODES: usize = 65_536;

/// Semantic slot facts that Arrow fields do not fully represent. An absent
/// unique ID is a known absence, distinct from an unspecified slot record.
#[derive(Clone, Debug, Eq, PartialEq)]
struct StaticSlotMetadata {
    field_schema: StaticFieldSchema,
    unique_id: Option<i32>,
}

impl StaticSlotMetadata {
    const fn field_schema(&self) -> &StaticFieldSchema {
        &self.field_schema
    }
}

/// Immutable Arrow schema plus exact execution slot order. No Chunk or lease
/// is retained by this type.
#[derive(Clone, Debug)]
pub struct StaticLayout {
    schema: SchemaRef,
    slots: Arc<[SlotId]>,
    slot_metadata: Option<Arc<[StaticSlotMetadata]>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LayoutError {
    ArityMismatch,
    DuplicateSlot,
    UnknownSlot,
    TooDeep,
    TooManyMetadataNodes,
    Encode,
}

impl fmt::Display for LayoutError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ArityMismatch => "static layout field and slot counts differ",
            Self::DuplicateSlot => "static layout contains a duplicate slot",
            Self::UnknownSlot => "static layout projection names an unknown slot",
            Self::TooDeep => "static layout slot metadata exceeds the depth limit",
            Self::TooManyMetadataNodes => "static layout slot metadata exceeds the node limit",
            Self::Encode => "static layout schema cannot be encoded canonically",
        })
    }
}

impl std::error::Error for LayoutError {}

impl StaticLayout {
    /// Schema-only construction explicitly records unknown slot semantics.
    /// Production lowering uses `try_new_exact` from the decoded ChunkSchema.
    pub fn try_new(schema: SchemaRef, slots: Arc<[SlotId]>) -> Result<Self, LayoutError> {
        Self::try_new_inner(schema, slots, None)
    }

    pub fn try_new_exact(
        schema: SchemaRef,
        slots: Arc<[SlotId]>,
        slot_metadata: Vec<(StaticFieldSchema, Option<i32>)>,
    ) -> Result<Self, LayoutError> {
        let metadata = slot_metadata
            .into_iter()
            .map(|(field_schema, unique_id)| StaticSlotMetadata {
                field_schema,
                unique_id,
            })
            .collect::<Vec<_>>();
        Self::try_new_inner(schema, slots, Some(Arc::from(metadata)))
    }

    fn try_new_inner(
        schema: SchemaRef,
        slots: Arc<[SlotId]>,
        slot_metadata: Option<Arc<[StaticSlotMetadata]>>,
    ) -> Result<Self, LayoutError> {
        if schema.fields().len() != slots.len() {
            return Err(LayoutError::ArityMismatch);
        }
        if slot_metadata
            .as_ref()
            .is_some_and(|metadata| metadata.len() != slots.len())
        {
            return Err(LayoutError::ArityMismatch);
        }
        let unique = slots.iter().copied().collect::<BTreeSet<_>>();
        if unique.len() != slots.len() {
            return Err(LayoutError::DuplicateSlot);
        }
        if let Some(metadata) = &slot_metadata {
            validate_metadata(metadata)?;
        }
        Ok(Self {
            schema,
            slots,
            slot_metadata,
        })
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn slots(&self) -> &[SlotId] {
        &self.slots
    }

    /// `None` means semantic slot metadata was not supplied, not that it was
    /// inferred empty from Arrow fields.
    pub fn has_exact_slot_metadata(&self) -> bool {
        self.slot_metadata.is_some()
    }

    pub fn slot_metadata_at(&self, index: usize) -> Option<(&StaticFieldSchema, Option<i32>)> {
        let metadata = self.slot_metadata.as_ref()?.get(index)?;
        Some((&metadata.field_schema, metadata.unique_id))
    }

    /// Empty output columns mean the complete input layout, matching the
    /// stream sink's existing projection semantics.
    pub fn project_by_slots(&self, output_columns: &[SlotId]) -> Result<Self, LayoutError> {
        if output_columns.is_empty() {
            return Ok(self.clone());
        }
        let index_by_slot = self
            .slots
            .iter()
            .copied()
            .enumerate()
            .map(|(index, slot)| (slot, index))
            .collect::<BTreeMap<_, _>>();
        let fields = output_columns
            .iter()
            .map(|slot| {
                let index = index_by_slot
                    .get(slot)
                    .copied()
                    .ok_or(LayoutError::UnknownSlot)?;
                Ok(self.schema.field(index).clone())
            })
            .collect::<Result<Vec<_>, LayoutError>>()?;
        let projected_schema = Arc::new(Schema::new_with_metadata(
            fields,
            self.schema.metadata().clone(),
        ));
        let projected_slots = Arc::from(output_columns);
        if let Some(metadata) = &self.slot_metadata {
            let projected_metadata = output_columns
                .iter()
                .map(|slot| {
                    let source = &metadata[index_by_slot[slot]];
                    (source.field_schema.clone(), source.unique_id)
                })
                .collect();
            Self::try_new_exact(projected_schema, projected_slots, projected_metadata)
        } else {
            Self::try_new(projected_schema, projected_slots)
        }
    }

    /// A deterministic identity over the full Arrow schema and exact slot order.
    /// Object keys are sorted explicitly, including Arrow metadata.
    pub fn identity(&self) -> Result<LayoutIdentity, LayoutError> {
        let schema = serde_json::to_value(self.schema.as_ref()).map_err(|_| LayoutError::Encode)?;
        let schema =
            serde_json::to_vec(&canonicalize_json(schema)).map_err(|_| LayoutError::Encode)?;
        let mut digest = Sha256::new();
        digest.update(b"novarocks-static-layout-v2");
        digest.update((schema.len() as u64).to_le_bytes());
        digest.update(schema);
        digest.update((self.slots.len() as u64).to_le_bytes());
        for (index, slot) in self.slots.iter().enumerate() {
            digest.update(slot.as_u32().to_le_bytes());
            if let Some(metadata) = &self.slot_metadata {
                digest.update([1]);
                match metadata[index].unique_id {
                    Some(unique_id) => {
                        digest.update([1]);
                        digest.update(unique_id.to_le_bytes());
                    }
                    None => digest.update([0]),
                }
                hash_field_schema(&mut digest, &metadata[index].field_schema);
            } else {
                digest.update([0]);
            }
        }
        Ok(LayoutIdentity::from_sha256(digest.finalize().into()))
    }
}

fn validate_metadata(metadata: &[StaticSlotMetadata]) -> Result<(), LayoutError> {
    let mut count = 0usize;
    let mut pending = metadata
        .iter()
        .map(|slot| (slot.field_schema(), 1usize))
        .collect::<Vec<_>>();
    while let Some((field, depth)) = pending.pop() {
        count += 1;
        if count > MAX_SLOT_METADATA_NODES {
            return Err(LayoutError::TooManyMetadataNodes);
        }
        if depth > MAX_SLOT_METADATA_DEPTH {
            return Err(LayoutError::TooDeep);
        }
        pending.extend(field.children().iter().map(|child| (child, depth + 1)));
    }
    Ok(())
}

fn hash_field_schema(digest: &mut Sha256, root: &StaticFieldSchema) {
    let mut pending = vec![root];
    while let Some(field) = pending.pop() {
        let tag = match field.logical_type() {
            None => 0,
            Some(LogicalType::Json) => 1,
            Some(LogicalType::Hll) => 2,
            Some(LogicalType::Bitmap) => 3,
            Some(LogicalType::Object) => 4,
            Some(LogicalType::Percentile) => 5,
        };
        digest.update([tag]);
        digest.update((field.children().len() as u64).to_le_bytes());
        pending.extend(field.children().iter().rev());
    }
}

fn canonicalize_json(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(object) => {
            let mut entries = object.into_iter().collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            let mut ordered = serde_json::Map::new();
            for (key, value) in entries {
                ordered.insert(key, canonicalize_json(value));
            }
            serde_json::Value::Object(ordered)
        }
        serde_json::Value::Array(array) => {
            serde_json::Value::Array(array.into_iter().map(canonicalize_json).collect())
        }
        scalar => scalar,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};

    #[test]
    fn rejects_layout_that_cannot_bind_columns_exactly() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]));
        assert_eq!(
            StaticLayout::try_new(Arc::clone(&schema), Arc::from([SlotId::new(1)])).unwrap_err(),
            LayoutError::ArityMismatch
        );
        assert_eq!(
            StaticLayout::try_new(schema, Arc::from([SlotId::new(1), SlotId::new(1)])).unwrap_err(),
            LayoutError::DuplicateSlot
        );
    }

    #[test]
    fn identity_includes_schema_and_slot_order() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]));
        let first = StaticLayout::try_new(
            Arc::clone(&schema),
            Arc::from([SlotId::new(1), SlotId::new(2)]),
        )
        .unwrap();
        let reordered =
            StaticLayout::try_new(schema, Arc::from([SlotId::new(2), SlotId::new(1)])).unwrap();
        assert_eq!(first.identity().unwrap(), first.identity().unwrap());
        assert_ne!(first.identity().unwrap(), reordered.identity().unwrap());
    }

    #[test]
    fn sink_projection_keeps_slot_order_and_rejects_unknown_slot() {
        let source = StaticLayout::try_new(
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int64, false),
                Field::new("b", DataType::Utf8, true),
            ])),
            Arc::from([SlotId::new(1), SlotId::new(2)]),
        )
        .unwrap();
        let projected = source.project_by_slots(&[SlotId::new(2)]).unwrap();
        assert_eq!(projected.slots(), &[SlotId::new(2)]);
        assert_eq!(projected.schema().field(0).name(), "b");
        assert_eq!(
            source.project_by_slots(&[]).unwrap().identity(),
            source.identity()
        );
        assert_eq!(
            source.project_by_slots(&[SlotId::new(3)]).unwrap_err(),
            LayoutError::UnknownSlot
        );
    }

    #[test]
    fn exact_slot_semantics_change_identity_without_arrow_schema_change() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, true)]));
        let slots = Arc::from([SlotId::new(7)]);
        let unspecified = StaticLayout::try_new(Arc::clone(&schema), Arc::clone(&slots)).unwrap();
        let plain = StaticLayout::try_new_exact(
            Arc::clone(&schema),
            Arc::clone(&slots),
            vec![(StaticFieldSchema::new(None, vec![]), None)],
        )
        .unwrap();
        let logical = StaticLayout::try_new_exact(
            Arc::clone(&schema),
            Arc::clone(&slots),
            vec![(
                StaticFieldSchema::new(Some(LogicalType::Json), vec![]),
                None,
            )],
        )
        .unwrap();
        let unique = StaticLayout::try_new_exact(
            Arc::clone(&schema),
            Arc::clone(&slots),
            vec![(StaticFieldSchema::new(None, vec![]), Some(3))],
        )
        .unwrap();
        assert!(!unspecified.has_exact_slot_metadata());
        assert!(plain.has_exact_slot_metadata());
        assert_ne!(unspecified.identity().unwrap(), plain.identity().unwrap());
        assert_ne!(plain.identity().unwrap(), logical.identity().unwrap());
        assert_ne!(plain.identity().unwrap(), unique.identity().unwrap());
        assert_eq!(
            logical.slot_metadata_at(0).unwrap().0.logical_type(),
            Some(LogicalType::Json)
        );
        assert_eq!(unique.slot_metadata_at(0).unwrap().1, Some(3));
    }

    #[test]
    fn projection_keeps_exact_nested_slot_metadata() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, true),
        ]));
        let nested = StaticFieldSchema::new(
            None,
            vec![StaticFieldSchema::new(Some(LogicalType::Bitmap), vec![])],
        );
        let source = StaticLayout::try_new_exact(
            schema,
            Arc::from([SlotId::new(1), SlotId::new(2)]),
            vec![
                (StaticFieldSchema::new(None, vec![]), None),
                (nested.clone(), Some(9)),
            ],
        )
        .unwrap();
        let projected = source.project_by_slots(&[SlotId::new(2)]).unwrap();
        assert_eq!(projected.slot_metadata_at(0), Some((&nested, Some(9))));
        assert_ne!(projected.identity().unwrap(), source.identity().unwrap());
        assert_eq!(
            source.project_by_slots(&[]).unwrap().identity(),
            source.identity()
        );
    }

    #[test]
    fn rejects_unbounded_nested_slot_metadata() {
        let mut field = StaticFieldSchema::new(None, vec![]);
        for _ in 0..MAX_SLOT_METADATA_DEPTH {
            field = StaticFieldSchema::new(None, vec![field]);
        }
        let result = StaticLayout::try_new_exact(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)])),
            Arc::from([SlotId::new(1)]),
            vec![(field, None)],
        );
        assert!(matches!(result, Err(LayoutError::TooDeep)));
    }
}
