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

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use arrow_schema::{Schema, SchemaRef};
use novarocks_type_contract::owned_resources::metadata_materialization::SchemaMetadataMaterializations;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, MAX_UNOBSERVED_COMPILE_WORK,
    PureCompileControl,
};
use novarocks_types::SlotId;
use novarocks_types::arrow_metadata_owner::{FieldMetadataOrigins, MetadataOwnedSchema};
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
#[derive(Clone)]
pub struct StaticLayout {
    schema: SchemaRef,
    slots: Arc<[SlotId]>,
    slot_metadata: Option<Arc<[StaticSlotMetadata]>>,
    metadata_materializations: Option<SchemaMetadataMaterializations>,
    field_metadata_origins: Option<FieldMetadataOrigins>,
    schema_metadata_origin: Option<MetadataOwnedSchema>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LayoutError {
    ArityMismatch,
    DuplicateSlot,
    UnknownSlot,
    TooDeep,
    TooManyMetadataNodes,
    Encode,
    MetadataOwnerConflict,
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
            Self::MetadataOwnerConflict => {
                "static layout metadata origins conflict with exact owners"
            }
        })
    }
}

impl std::error::Error for LayoutError {}

/// A compilation failure keeps its original control cause separate from layout
/// diagnostics. These APIs authorize neither allocation nor retained memory.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LayoutCompileError {
    Layout(LayoutError),
    Control(CompileControlError),
}
impl From<LayoutError> for LayoutCompileError {
    fn from(error: LayoutError) -> Self {
        Self::Layout(error)
    }
}
impl From<CompileControlError> for LayoutCompileError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl fmt::Display for LayoutCompileError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Layout(error) => error.fmt(formatter),
            Self::Control(error) => error.fmt(formatter),
        }
    }
}
impl std::error::Error for LayoutCompileError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Layout(error) => Some(error),
            Self::Control(error) => Some(error),
        }
    }
}

// The absent observer belongs only to the existing legacy entry points. A
// compile refusal never switches to that mode. Serde, Arrow deep clones and
// container allocation are opaque operations with observations on both sides;
// their internal cooperation and first allocation are not proven here.
struct LayoutWork<'a>(Option<CompileCheckpoints<'a>>);
impl LayoutWork<'_> {
    fn step(&mut self) -> Result<(), LayoutCompileError> {
        if let Some(work) = &mut self.0 {
            work.step()?;
        }
        Ok(())
    }
    fn flush(&mut self) -> Result<(), LayoutCompileError> {
        if let Some(work) = &mut self.0 {
            work.flush()?;
        }
        Ok(())
    }
    fn opaque<T>(
        &mut self,
        operation: impl FnOnce() -> Result<T, LayoutError>,
    ) -> Result<T, LayoutCompileError> {
        self.flush()?;
        let result = operation();
        self.flush()?;
        result.map_err(LayoutCompileError::Layout)
    }
    fn hash_bytes(&mut self, digest: &mut Sha256, bytes: &[u8]) -> Result<(), LayoutCompileError> {
        for chunk in bytes.chunks(MAX_UNOBSERVED_COMPILE_WORK as usize) {
            self.flush()?;
            digest.update(chunk);
            for _ in chunk {
                self.step()?;
            }
        }
        Ok(())
    }
}
fn compile<T>(
    control: &dyn PureCompileControl,
    operation: impl FnOnce(&mut LayoutWork<'_>) -> Result<T, LayoutCompileError>,
) -> Result<T, LayoutCompileError> {
    let mut work = LayoutWork(Some(CompileCheckpoints::try_new(
        control,
        CompilePhase::LowerProgram,
    )?));
    let result = operation(&mut work);
    if matches!(&result, Err(LayoutCompileError::Control(_))) {
        return result;
    }
    work.flush()?;
    result
}
fn legacy<T>(
    operation: impl FnOnce(&mut LayoutWork<'_>) -> Result<T, LayoutCompileError>,
) -> Result<T, LayoutError> {
    match operation(&mut LayoutWork(None)) {
        Ok(value) => Ok(value),
        Err(LayoutCompileError::Layout(error)) => Err(error),
        Err(LayoutCompileError::Control(_)) => {
            unreachable!("legacy layout has no compile observer")
        }
    }
}

impl StaticLayout {
    /// Schema-only construction explicitly records unknown slot semantics.
    /// Production lowering uses `try_new_exact` from the decoded ChunkSchema.
    pub fn try_new(schema: SchemaRef, slots: Arc<[SlotId]>) -> Result<Self, LayoutError> {
        legacy(|work| Self::try_new_inner(schema, slots, None, work))
    }
    pub fn try_new_for_compile(
        schema: SchemaRef,
        slots: Arc<[SlotId]>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, LayoutCompileError> {
        compile(control, |work| {
            Self::try_new_inner(schema, slots, None, work)
        })
    }
    pub fn try_new_exact(
        schema: SchemaRef,
        slots: Arc<[SlotId]>,
        slot_metadata: Vec<(StaticFieldSchema, Option<i32>)>,
    ) -> Result<Self, LayoutError> {
        legacy(|work| Self::try_new_exact_inner(schema, slots, slot_metadata, work))
    }
    pub fn try_new_exact_for_compile(
        schema: SchemaRef,
        slots: Arc<[SlotId]>,
        slot_metadata: Vec<(StaticFieldSchema, Option<i32>)>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, LayoutCompileError> {
        compile(control, |work| {
            Self::try_new_exact_inner(schema, slots, slot_metadata, work)
        })
    }
    fn try_new_exact_inner(
        schema: SchemaRef,
        slots: Arc<[SlotId]>,
        slot_metadata: Vec<(StaticFieldSchema, Option<i32>)>,
        work: &mut LayoutWork<'_>,
    ) -> Result<Self, LayoutCompileError> {
        let mut metadata = Vec::with_capacity(slot_metadata.len());
        for (field_schema, unique_id) in slot_metadata {
            metadata.push(StaticSlotMetadata {
                field_schema,
                unique_id,
            });
            work.step()?;
        }
        let metadata = work.opaque(|| Ok(Arc::from(metadata)))?;
        Self::try_new_inner(schema, slots, Some(metadata), work)
    }
    fn try_new_inner(
        schema: SchemaRef,
        slots: Arc<[SlotId]>,
        slot_metadata: Option<Arc<[StaticSlotMetadata]>>,
        work: &mut LayoutWork<'_>,
    ) -> Result<Self, LayoutCompileError> {
        let arity_matches = schema.fields().len() == slots.len()
            && slot_metadata
                .as_ref()
                .is_none_or(|metadata| metadata.len() == slots.len());
        work.step()?;
        if !arity_matches {
            return Err(LayoutError::ArityMismatch.into());
        }
        let mut unique = BTreeSet::new();
        for slot in slots.iter().copied() {
            unique.insert(slot);
            work.step()?;
        }
        if unique.len() != slots.len() {
            return Err(LayoutError::DuplicateSlot.into());
        }
        if let Some(metadata) = &slot_metadata {
            validate_metadata(metadata, work)?;
        }
        Ok(Self {
            schema,
            slots,
            slot_metadata,
            metadata_materializations: None,
            field_metadata_origins: None,
            schema_metadata_origin: None,
        })
    }
    /// Consume the positively paired original Schema owner. No public API
    /// accepts an unrelated schema plus a fabricated numerical receipt.
    pub fn try_new_materialized_for_compile(
        metadata_materializations: SchemaMetadataMaterializations,
        slots: Arc<[SlotId]>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, LayoutCompileError> {
        let schema = metadata_materializations.schema_owner().schema().clone();
        let mut layout = Self::try_new_for_compile(schema, slots, control)?;
        layout.metadata_materializations = Some(metadata_materializations);
        Ok(layout)
    }
    pub fn metadata_materializations(&self) -> Option<&SchemaMetadataMaterializations> {
        self.metadata_materializations.as_ref()
    }
    pub fn with_metadata_origins(
        mut self,
        fields: FieldMetadataOrigins,
        schema: Option<MetadataOwnedSchema>,
    ) -> Result<Self, LayoutError> {
        if self
            .schema
            .fields()
            .iter()
            .any(|field| fields.metadata_bytes_for(field).is_none())
            || schema
                .as_ref()
                .is_some_and(|origin| origin.backing_bytes_for(&self.schema).is_none())
        {
            return Err(LayoutError::MetadataOwnerConflict);
        }
        self.field_metadata_origins = Some(fields);
        self.schema_metadata_origin = schema;
        Ok(self)
    }

    pub fn field_metadata_origins(&self) -> Option<&FieldMetadataOrigins> {
        self.field_metadata_origins.as_ref()
    }
    pub fn schema_metadata_origin(&self) -> Option<&MetadataOwnedSchema> {
        self.schema_metadata_origin.as_ref()
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
        legacy(|work| self.project_inner(output_columns, work))
    }
    pub fn project_by_slots_for_compile(
        &self,
        output_columns: &[SlotId],
        control: &dyn PureCompileControl,
    ) -> Result<Self, LayoutCompileError> {
        compile(control, |work| self.project_inner(output_columns, work))
    }
    fn project_inner(
        &self,
        output_columns: &[SlotId],
        work: &mut LayoutWork<'_>,
    ) -> Result<Self, LayoutCompileError> {
        if output_columns.is_empty() {
            return Ok(self.clone());
        }
        let mut index_by_slot = BTreeMap::new();
        for (index, slot) in self.slots.iter().copied().enumerate() {
            index_by_slot.insert(slot, index);
            work.step()?;
        }
        // Preserve the exact FieldRef owners used by the root metadata proof.
        // This branch consumes actual origins; it never relabels a reconstructed
        // Field as the original one. The original materialization-only path below
        // keeps its original clone author and observation sequence.
        if let Some(origins) = &self.field_metadata_origins {
            let mut fields = Vec::with_capacity(output_columns.len());
            let mut projected_slots = Vec::with_capacity(output_columns.len());
            let mut projected_metadata = self
                .slot_metadata
                .as_ref()
                .map(|_| Vec::with_capacity(output_columns.len()));
            for slot in output_columns {
                let index = index_by_slot.get(slot).copied();
                work.step()?;
                let index = index.ok_or(LayoutError::UnknownSlot)?;
                fields.push(Arc::clone(&self.schema.fields()[index]));
                projected_slots.push(*slot);
                if let (Some(source), Some(destination)) =
                    (&self.slot_metadata, &mut projected_metadata)
                {
                    destination.push(work.opaque(|| Ok(source[index].clone()))?);
                }
                work.step()?;
            }
            let mut projected_materializations = None;
            let (schema, schema_origin) = if let Some(source) = &self.metadata_materializations {
                let projected =
                    work.opaque(|| Ok(source.project_shared_fields_original(fields)))?;
                let schema = Arc::clone(projected.schema_owner().schema());
                projected_materializations = Some(projected);
                // The original UEA constructor owns this new root table. An
                // old M07 schema receipt cannot describe the new Schema Arc.
                (schema, None)
            } else if let Some(source) = &self.schema_metadata_origin {
                let derived = work.opaque(|| {
                    source
                        .derive_schema(
                            fields.into(),
                            novarocks_types::arrow_metadata_owner::MetadataOwnerLimits {
                                entries: 65536,
                                construction_bytes: 96 * 1024 * 1024,
                            },
                        )
                        .map_err(|_| LayoutError::MetadataOwnerConflict.into())
                })?;
                (Arc::clone(derived.schema()), Some(derived))
            } else {
                (
                    work.opaque(|| {
                        Ok(Arc::new(Schema::new_with_metadata(
                            fields,
                            self.schema.metadata().clone(),
                        )))
                    })?,
                    None,
                )
            };
            let slots = work.opaque(|| Ok(Arc::from(projected_slots)))?;
            let metadata = match projected_metadata {
                Some(metadata) => Some(work.opaque(|| Ok(Arc::from(metadata)))?),
                None => None,
            };
            let mut projected = Self::try_new_inner(schema, slots, metadata, work)?
                .with_metadata_origins(origins.clone(), schema_origin)?;
            projected.metadata_materializations = projected_materializations;
            return Ok(projected);
        }
        let mut fields = if self.metadata_materializations.is_some() {
            Vec::new()
        } else {
            Vec::with_capacity(output_columns.len())
        };
        let mut materialized_fields = self
            .metadata_materializations
            .as_ref()
            .map(|_| Vec::with_capacity(output_columns.len()));
        for slot in output_columns {
            let index = index_by_slot.get(slot).copied();
            work.step()?;
            let index = index.ok_or(LayoutError::UnknownSlot)?;
            if let (Some(origins), Some(materialized)) = (
                self.metadata_materializations.as_ref(),
                materialized_fields.as_mut(),
            ) {
                work.flush()?;
                let field = origins
                    .clone_field_original_observed(&self.schema.fields()[index], &mut || {
                        work.step()
                    })?;
                work.flush()?;
                materialized.push(field);
            } else {
                let field = work.opaque(|| Ok(self.schema.field(index).clone()))?;
                fields.push(field);
            }
            work.step()?;
        }
        let projected_materializations = if let (Some(origins), Some(fields)) =
            (self.metadata_materializations.as_ref(), materialized_fields)
        {
            Some(work.opaque(|| Ok(origins.project_original_schema(fields)))?)
        } else {
            None
        };
        let projected_schema = if let Some(origins) = &projected_materializations {
            origins.schema_owner().schema().clone()
        } else {
            work.opaque(|| {
                Ok(Arc::new(Schema::new_with_metadata(
                    fields,
                    self.schema.metadata().clone(),
                )))
            })?
        };
        let mut projected_slots = Vec::with_capacity(output_columns.len());
        for slot in output_columns {
            projected_slots.push(*slot);
            work.step()?;
        }
        let projected_slots = work.opaque(|| Ok(Arc::from(projected_slots)))?;
        let projected_metadata = if let Some(metadata) = &self.slot_metadata {
            let mut projected = Vec::with_capacity(output_columns.len());
            for slot in output_columns {
                let source = &metadata[index_by_slot[slot]];
                let cloned = work.opaque(|| Ok(source.clone()))?;
                projected.push(cloned);
                work.step()?;
            }
            Some(work.opaque(|| Ok(Arc::from(projected)))?)
        } else {
            None
        };
        let mut projected =
            Self::try_new_inner(projected_schema, projected_slots, projected_metadata, work)?;
        projected.metadata_materializations = projected_materializations;
        Ok(projected)
    }
    /// A deterministic identity over the full Arrow schema and exact slot order.
    /// Object keys are sorted explicitly, including Arrow metadata.
    pub fn identity(&self) -> Result<LayoutIdentity, LayoutError> {
        legacy(|work| self.identity_inner(work))
    }
    pub fn identity_for_compile(
        &self,
        control: &dyn PureCompileControl,
    ) -> Result<LayoutIdentity, LayoutCompileError> {
        compile(control, |work| self.identity_inner(work))
    }
    fn identity_inner(
        &self,
        work: &mut LayoutWork<'_>,
    ) -> Result<LayoutIdentity, LayoutCompileError> {
        let schema = work.opaque(|| {
            serde_json::to_value(self.schema.as_ref()).map_err(|_| LayoutError::Encode)
        })?;
        let schema = canonicalize_json(schema, work)?;
        let schema =
            work.opaque(|| serde_json::to_vec(&schema).map_err(|_| LayoutError::Encode))?;
        let mut digest = Sha256::new();
        digest.update(b"novarocks-static-layout-v2");
        digest.update((schema.len() as u64).to_le_bytes());
        work.step()?;
        work.hash_bytes(&mut digest, &schema)?;
        digest.update((self.slots.len() as u64).to_le_bytes());
        work.step()?;
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
                work.step()?;
                hash_field_schema(&mut digest, &metadata[index].field_schema, work)?;
            } else {
                digest.update([0]);
                work.step()?;
            }
        }
        let digest = digest.finalize().into();
        work.step()?;
        Ok(LayoutIdentity::from_sha256(digest))
    }
}
fn validate_metadata(
    metadata: &[StaticSlotMetadata],
    work: &mut LayoutWork<'_>,
) -> Result<(), LayoutCompileError> {
    let mut count = 0usize;
    let mut pending = Vec::with_capacity(metadata.len());
    for slot in metadata {
        pending.push((slot.field_schema(), 1usize));
        work.step()?;
    }
    while let Some((field, depth)) = pending.pop() {
        count += 1;
        work.step()?;
        if count > MAX_SLOT_METADATA_NODES {
            return Err(LayoutError::TooManyMetadataNodes.into());
        }
        if depth > MAX_SLOT_METADATA_DEPTH {
            return Err(LayoutError::TooDeep.into());
        }
        for child in field.children() {
            pending.push((child, depth + 1));
            work.step()?;
        }
    }
    Ok(())
}
fn hash_field_schema(
    digest: &mut Sha256,
    root: &StaticFieldSchema,
    work: &mut LayoutWork<'_>,
) -> Result<(), LayoutCompileError> {
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
        work.step()?;
        for child in field.children().iter().rev() {
            pending.push(child);
            work.step()?;
        }
    }
    Ok(())
}
fn compare_keys(
    left: &str,
    right: &str,
    work: &mut LayoutWork<'_>,
) -> Result<Ordering, LayoutCompileError> {
    for (left, right) in left.bytes().zip(right.bytes()) {
        let ordering = left.cmp(&right);
        work.step()?;
        if ordering != Ordering::Equal {
            return Ok(ordering);
        }
    }
    let ordering = left.len().cmp(&right.len());
    work.step()?;
    Ok(ordering)
}
// Fallible in-place lexical heapsort preserves the previous canonical key
// order without an opaque comparison callback that could swallow Control.
fn sort_entries(
    entries: &mut [(String, serde_json::Value)],
    work: &mut LayoutWork<'_>,
) -> Result<(), LayoutCompileError> {
    fn sift(
        entries: &mut [(String, serde_json::Value)],
        mut root: usize,
        end: usize,
        work: &mut LayoutWork<'_>,
    ) -> Result<(), LayoutCompileError> {
        while root < end / 2 {
            let mut child = root * 2 + 1;
            if child + 1 < end
                && compare_keys(&entries[child].0, &entries[child + 1].0, work)? == Ordering::Less
            {
                child += 1;
            }
            if compare_keys(&entries[root].0, &entries[child].0, work)? != Ordering::Less {
                break;
            }
            entries.swap(root, child);
            work.step()?;
            root = child;
        }
        Ok(())
    }
    let length = entries.len();
    for root in (0..length / 2).rev() {
        sift(entries, root, length, work)?;
    }
    for end in (1..entries.len()).rev() {
        entries.swap(0, end);
        work.step()?;
        sift(entries, 0, end, work)?;
    }
    Ok(())
}
fn canonicalize_json(
    value: serde_json::Value,
    work: &mut LayoutWork<'_>,
) -> Result<serde_json::Value, LayoutCompileError> {
    match value {
        serde_json::Value::Object(object) => {
            let mut entries = Vec::with_capacity(object.len());
            for entry in object {
                entries.push(entry);
                work.step()?;
            }
            sort_entries(&mut entries, work)?;
            let mut ordered = serde_json::Map::new();
            for (key, value) in entries {
                let value = canonicalize_json(value, work)?;
                work.opaque(|| {
                    ordered.insert(key, value);
                    Ok(())
                })?;
                work.step()?;
            }
            Ok(serde_json::Value::Object(ordered))
        }
        serde_json::Value::Array(array) => {
            let mut ordered = Vec::with_capacity(array.len());
            for value in array {
                ordered.push(canonicalize_json(value, work)?);
                work.step()?;
            }
            Ok(serde_json::Value::Array(ordered))
        }
        scalar => {
            work.step()?;
            Ok(scalar)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};

    #[test]
    fn projection_keeps_exact_field_arcs_and_derives_only_known_schema_metadata() {
        use novarocks_types::arrow_metadata_owner::{ArrowMetadataOwner, MetadataOwnerLimits};
        let limits = MetadataOwnerLimits {
            entries: 1,
            construction_bytes: 4096,
        };
        let first = ArrowMetadataOwner::try_new(vec![], limits)
            .unwrap()
            .into_field("a".into(), DataType::Int64, false);
        let second = ArrowMetadataOwner::try_new(vec![("source".into(), "known".into())], limits)
            .unwrap()
            .into_field("b".into(), DataType::Utf8, true);
        let top = ArrowMetadataOwner::try_new(vec![("schema".into(), "known".into())], limits)
            .unwrap()
            .into_schema(vec![Arc::clone(first.field()), Arc::clone(second.field())].into());
        let bare = StaticLayout::try_new(
            Arc::clone(top.schema()),
            Arc::from([SlotId::new(1), SlotId::new(2)]),
        )
        .unwrap();
        let source = bare
            .clone()
            .with_metadata_origins(
                FieldMetadataOrigins::try_new(vec![first, second], 2).unwrap(),
                Some(top),
            )
            .unwrap();
        assert_eq!(bare.identity(), source.identity());
        let projected = source.project_by_slots(&[SlotId::new(2)]).unwrap();
        assert!(Arc::ptr_eq(
            &source.schema().fields()[1],
            &projected.schema().fields()[0]
        ));
        assert!(!Arc::ptr_eq(source.schema(), projected.schema()));
        assert_eq!(source.schema().metadata(), projected.schema().metadata());
        assert!(
            projected
                .schema_metadata_origin()
                .unwrap()
                .backing_bytes_for(projected.schema())
                .is_some()
        );
        assert!(
            source
                .schema_metadata_origin()
                .unwrap()
                .backing_bytes_for(projected.schema())
                .is_none()
        );
        let unknown = bare.project_by_slots(&[SlotId::new(2)]).unwrap();
        assert!(unknown.field_metadata_origins().is_none());
        assert!(unknown.schema_metadata_origin().is_none());
        assert_eq!(unknown.identity(), projected.identity());
        // Equal values in a new allocation cannot borrow the original receipt.
        let independent = StaticLayout::try_new(
            Arc::new(source.schema().as_ref().clone()),
            Arc::from(source.slots()),
        )
        .unwrap();
        assert_eq!(
            independent
                .with_metadata_origins(
                    source.field_metadata_origins().unwrap().clone(),
                    source.schema_metadata_origin().cloned()
                )
                .unwrap_err(),
            LayoutError::MetadataOwnerConflict
        );
    }

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
    fn identity_distinguishes_complete_raw_domains_with_the_same_legacy_cache() {
        use novarocks_type_contract::NR_LOGICAL_TYPE_KEY;
        let layout = |marker: Option<&str>| {
            let mut field = Field::new("value", DataType::FixedSizeBinary(16), true);
            if let Some(marker) = marker {
                field = field.with_metadata([(NR_LOGICAL_TYPE_KEY.into(), marker.into())].into());
            }
            StaticLayout::try_new_exact(
                Arc::new(Schema::new(vec![field])),
                Arc::from([SlotId::new(1)]),
                vec![(StaticFieldSchema::new(None, vec![]), None)],
            )
            .unwrap()
        };
        let physical = layout(None).identity().unwrap();
        let large = layout(Some("largeint")).identity().unwrap();
        let uuid = layout(Some("uuid")).identity().unwrap();
        assert_ne!(physical, large);
        assert_ne!(physical, uuid);
        assert_ne!(large, uuid);
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

    #[derive(Default)]
    struct OriginalControl {
        trace: std::sync::Mutex<Vec<(CompilePhase, u32)>>,
        stop: Option<(usize, CompileControlError)>,
    }
    impl PureCompileControl for OriginalControl {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            let mut trace = self.trace.lock().unwrap();
            let index = trace.len();
            trace.push((phase, units));
            if let Some((at, cause)) = self.stop
                && at == index
            {
                return Err(cause);
            }
            Ok(())
        }
    }
    fn causes() -> [CompileControlError; 3] {
        [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ]
    }
    fn wide_layout_input(count: u32) -> (SchemaRef, Arc<[SlotId]>) {
        let fields = (0..count)
            .map(|index| Field::new(format!("v{index}"), DataType::Int64, false))
            .collect::<Vec<_>>();
        let slots = (0..count).map(SlotId::new).collect::<Vec<_>>();
        (Arc::new(Schema::new(fields)), Arc::from(slots))
    }

    #[test]
    fn compile_layout_preserves_projection_schema_and_known_absence() {
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("provider".to_string(), "exact-source".to_string());
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("a", DataType::Int64, false),
                Field::new("b", DataType::Utf8, true).with_metadata(metadata.clone()),
            ],
            metadata,
        ));
        let slots: Arc<[SlotId]> = Arc::from([SlotId::new(9), SlotId::new(3)]);
        let nested = StaticFieldSchema::new(
            None,
            vec![StaticFieldSchema::new(Some(LogicalType::Json), vec![])],
        );
        let facts = vec![
            (StaticFieldSchema::new(None, vec![]), None),
            (nested.clone(), Some(17)),
        ];
        let legacy =
            StaticLayout::try_new_exact(schema.clone(), slots.clone(), facts.clone()).unwrap();
        let control = OriginalControl::default();
        let checked =
            StaticLayout::try_new_exact_for_compile(schema.clone(), slots.clone(), facts, &control)
                .unwrap();
        assert!(Arc::ptr_eq(checked.schema(), &schema));
        assert_eq!(
            checked.identity_for_compile(&control).unwrap(),
            legacy.identity().unwrap()
        );
        let projected = checked
            .project_by_slots_for_compile(&[SlotId::new(3)], &control)
            .unwrap();
        assert_eq!(projected.slots(), &[SlotId::new(3)]);
        assert_eq!(projected.schema().field(0), schema.field(1));
        assert_eq!(projected.schema().metadata(), schema.metadata());
        assert_eq!(projected.slot_metadata_at(0), Some((&nested, Some(17))));
        assert_eq!(
            projected.identity_for_compile(&control).unwrap(),
            legacy
                .project_by_slots(&[SlotId::new(3)])
                .unwrap()
                .identity()
                .unwrap()
        );
        let unknown = StaticLayout::try_new_for_compile(schema, slots, &control).unwrap();
        assert!(!unknown.has_exact_slot_metadata());
        assert_ne!(
            unknown.identity_for_compile(&control).unwrap(),
            checked.identity_for_compile(&control).unwrap()
        );
        assert!(
            control
                .trace
                .lock()
                .unwrap()
                .iter()
                .all(|(phase, units)| *phase == CompilePhase::LowerProgram && *units <= 256)
        );
    }

    #[test]
    fn compile_layout_all_entry_points_refuse_original_control_before_work() {
        let (schema, slots) = wide_layout_input(1);
        let layout = StaticLayout::try_new(schema.clone(), slots.clone()).unwrap();
        for cause in causes() {
            for entry in 0..4 {
                let control = OriginalControl {
                    trace: Default::default(),
                    stop: Some((0, cause)),
                };
                let result = match entry {
                    0 => StaticLayout::try_new_for_compile(schema.clone(), slots.clone(), &control)
                        .map(|_| ()),
                    1 => StaticLayout::try_new_exact_for_compile(
                        schema.clone(),
                        slots.clone(),
                        vec![(StaticFieldSchema::new(None, vec![]), None)],
                        &control,
                    )
                    .map(|_| ()),
                    2 => layout
                        .project_by_slots_for_compile(&[], &control)
                        .map(|_| ()),
                    _ => layout.identity_for_compile(&control).map(|_| ()),
                };
                assert_eq!(result, Err(LayoutCompileError::Control(cause)));
                assert_eq!(
                    *control.trace.lock().unwrap(),
                    vec![(CompilePhase::LowerProgram, 0)]
                );
            }
        }
    }

    #[test]
    fn compile_layout_slot_work_refuses_at_quantum_without_publication_or_recheck() {
        let (schema, slots) = wide_layout_input(300);
        let baseline = OriginalControl::default();
        let layout =
            StaticLayout::try_new_for_compile(schema.clone(), slots.clone(), &baseline).unwrap();
        assert_eq!(layout.slots().len(), 300);
        let trace = baseline.trace.lock().unwrap().clone();
        assert_eq!(trace[1], (CompilePhase::LowerProgram, 256));
        assert!(trace.last().unwrap().1 > 0 && trace.last().unwrap().1 < 256);
        for cause in causes() {
            for at in [1, trace.len() - 1] {
                let control = OriginalControl {
                    trace: Default::default(),
                    stop: Some((at, cause)),
                };
                assert!(
                    matches!(StaticLayout::try_new_for_compile(schema.clone(), slots.clone(), &control), Err(LayoutCompileError::Control(actual)) if actual == cause)
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }

    #[test]
    fn compile_layout_ordinary_error_tail_preserves_typed_control_priority() {
        use std::error::Error;
        assert!(
            LayoutCompileError::Layout(LayoutError::UnknownSlot)
                .source()
                .unwrap()
                .downcast_ref::<LayoutError>()
                .is_some()
        );
        assert_eq!(
            LayoutCompileError::Control(CompileControlError::Cancelled)
                .source()
                .unwrap()
                .downcast_ref::<CompileControlError>(),
            Some(&CompileControlError::Cancelled)
        );
        let (schema, slots) = wide_layout_input(1);
        let baseline = OriginalControl::default();
        assert!(matches!(
            StaticLayout::try_new_for_compile(schema.clone(), Arc::from([]), &baseline),
            Err(LayoutCompileError::Layout(LayoutError::ArityMismatch))
        ));
        assert_eq!(
            *baseline.trace.lock().unwrap(),
            vec![
                (CompilePhase::LowerProgram, 0),
                (CompilePhase::LowerProgram, 1)
            ]
        );
        let layout = StaticLayout::try_new(schema.clone(), slots).unwrap();
        let ordinary = OriginalControl::default();
        assert!(matches!(
            layout.project_by_slots_for_compile(&[SlotId::new(99)], &ordinary),
            Err(LayoutCompileError::Layout(LayoutError::UnknownSlot))
        ));
        let trace = ordinary.trace.lock().unwrap().clone();
        for cause in causes() {
            let control = OriginalControl {
                trace: Default::default(),
                stop: Some((1, cause)),
            };
            assert!(
                matches!(StaticLayout::try_new_for_compile(schema.clone(), Arc::from([]), &control), Err(LayoutCompileError::Control(actual)) if actual == cause)
            );
            assert_eq!(
                *control.trace.lock().unwrap(),
                *baseline.trace.lock().unwrap()
            );
            let control = OriginalControl {
                trace: Default::default(),
                stop: Some((trace.len() - 1, cause)),
            };
            assert!(
                matches!(layout.project_by_slots_for_compile(&[SlotId::new(99)], &control), Err(LayoutCompileError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace);
        }
    }

    #[test]
    fn compile_layout_empty_projection_still_observes_completion() {
        let (schema, slots) = wide_layout_input(1);
        let layout = StaticLayout::try_new(schema, slots).unwrap();
        for cause in causes() {
            let control = OriginalControl {
                trace: Default::default(),
                stop: Some((1, cause)),
            };
            assert!(
                matches!(layout.project_by_slots_for_compile(&[], &control), Err(LayoutCompileError::Control(actual)) if actual == cause)
            );
            assert_eq!(
                *control.trace.lock().unwrap(),
                vec![
                    (CompilePhase::LowerProgram, 0),
                    (CompilePhase::LowerProgram, 0)
                ]
            );
        }
    }

    #[test]
    fn compile_layout_identity_observes_actual_long_schema_hash_and_completion() {
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("source".to_string(), "x".repeat(4097));
        let layout = StaticLayout::try_new(
            Arc::new(Schema::new_with_metadata(
                vec![Field::new("v", DataType::Int64, false)],
                metadata,
            )),
            Arc::from([SlotId::new(4)]),
        )
        .unwrap();
        let baseline = OriginalControl::default();
        assert_eq!(
            layout.identity_for_compile(&baseline).unwrap(),
            layout.identity().unwrap()
        );
        let trace = baseline.trace.lock().unwrap().clone();
        let quantum = trace.iter().position(|(_, units)| *units == 256).unwrap();
        let tail = trace.len() - 1;
        assert!(trace[tail].1 > 0 && trace[tail].1 < 256);
        for cause in causes() {
            for at in [quantum, tail] {
                let control = OriginalControl {
                    trace: Default::default(),
                    stop: Some((at, cause)),
                };
                assert_eq!(
                    layout.identity_for_compile(&control),
                    Err(LayoutCompileError::Control(cause))
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }

    #[test]
    fn compile_layout_canonical_key_sort_has_independent_lexical_oracle_and_control() {
        let prefix = "a".repeat(300);
        let keys = [
            format!("{prefix}z"),
            "".into(),
            "é".into(),
            format!("{prefix}a"),
            "b".into(),
        ];
        let mut expected = keys.to_vec();
        expected.sort();
        let entries = || {
            keys.iter()
                .cloned()
                .map(|key| (key, serde_json::Value::Null))
                .collect::<Vec<_>>()
        };
        let baseline = OriginalControl::default();
        let mut actual = entries();
        compile(&baseline, |work| sort_entries(&mut actual, work)).unwrap();
        assert_eq!(
            actual
                .iter()
                .map(|entry| entry.0.clone())
                .collect::<Vec<_>>(),
            expected
        );
        let trace = baseline.trace.lock().unwrap().clone();
        let quantum = trace.iter().position(|(_, units)| *units == 256).unwrap();
        for cause in causes() {
            let control = OriginalControl {
                trace: Default::default(),
                stop: Some((quantum, cause)),
            };
            assert_eq!(
                compile(&control, |work| sort_entries(&mut entries(), work)),
                Err(LayoutCompileError::Control(cause))
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=quantum]);
        }
    }
}

impl std::fmt::Debug for StaticLayout {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        out.debug_struct("StaticLayout")
            .field("schema", &self.schema)
            .field("slots", &self.slots)
            .field("slot_metadata", &self.slot_metadata)
            .finish()
    }
}
