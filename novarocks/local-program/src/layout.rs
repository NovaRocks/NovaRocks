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
use novarocks_type_contract::{
    TypedValueBinding, ValueTypeRef, ValueTypeStorageLifetime, write_value_type_identity_observed,
};
use novarocks_types::SlotId;
use novarocks_types::arrow_metadata_owner::{FieldMetadataOrigins, MetadataOwnedSchema};
use sha2::{Digest, Sha256};

use crate::LayoutIdentity;

pub(crate) const MAX_SLOT_METADATA_DEPTH: usize = 64;
pub(crate) const MAX_SLOT_METADATA_NODES: usize = 65_536;

/// Semantic slot facts that Arrow fields do not fully represent. An absent
/// unique ID is a known absence, distinct from an unspecified slot record.
#[derive(Clone, Debug, Eq, PartialEq)]
struct StaticSlotMetadata {
    field_schema: ValueTypeRef,
    binding: Option<TypedValueBinding>,
    unique_id: Option<i32>,
}

impl StaticSlotMetadata {
    const fn field_schema(&self) -> &ValueTypeRef {
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
    slot_index: Option<Arc<[(SlotId, usize)]>>,
    typed_representation: bool,
    storage: ValueTypeStorageLifetime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LayoutError {
    ArityMismatch,
    DuplicateSlot,
    UnknownSlot,
    TooDeep,
    TooManyMetadataNodes,
    Encode,
    MetadataOwnerConflict,
    ValueBindingMismatch,
    TypedProjectionRequiresOwner,
    AllocationModel,
    ValueType(novarocks_type_contract::ExecutionValueTypeError),
    Resource(novarocks_type_contract::owned_resources::layout::LayoutResourceError),
    Encoding(novarocks_failure_contract::SharedFailureCause),
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
            Self::ValueBindingMismatch => "static layout conflicts with its explicit value binding",
            Self::TypedProjectionRequiresOwner => "typed static layout projection requires the creating owner's storage and request port",
            Self::AllocationModel => "static layout allocation request cannot be represented",
            Self::ValueType(source) => return source.fmt(formatter),
            Self::Resource(source) => return source.fmt(formatter),
            Self::Encoding(source) => return source.fmt(formatter),
        })
    }
}

impl std::error::Error for LayoutError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::ValueType(source) => Some(source),
            Self::Resource(source) => Some(source),
            Self::Encoding(source) => Some(source),
            _ => None,
        }
    }
}

/// A compilation failure keeps its original control cause separate from layout
/// diagnostics. These APIs authorize neither allocation nor retained memory.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LayoutCompileError {
    Layout(LayoutError),
    Control(CompileControlError),
}
#[derive(Debug)]
struct LayoutEncodingFailure(serde_json::Error);
impl fmt::Display for LayoutEncodingFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "canonical layout encoding failed: {}", self.0)
    }
}
impl std::error::Error for LayoutEncodingFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}
impl novarocks_failure_contract::FailureProjection for LayoutEncodingFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::*;
        FailureFact::new(
            FailureClass::Unexpected,
            FailureReason::new(
                domain::LOCAL_PROGRAM,
                std::num::NonZeroU16::new(211).unwrap(),
            ),
        )
    }
}
impl LayoutError {
    fn encoding(source: serde_json::Error) -> Self {
        Self::Encoding(novarocks_failure_contract::SharedFailureCause::new(
            LayoutEncodingFailure(source),
        ))
    }
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
impl From<novarocks_type_contract::ExecutionValueTypeError> for LayoutCompileError {
    fn from(source: novarocks_type_contract::ExecutionValueTypeError) -> Self {
        Self::Layout(LayoutError::ValueType(source))
    }
}
impl From<novarocks_type_contract::owned_resources::layout::LayoutResourceError>
    for LayoutCompileError
{
    fn from(source: novarocks_type_contract::owned_resources::layout::LayoutResourceError) -> Self {
        Self::Layout(LayoutError::Resource(source))
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

/// Creator refusals and original compile-control/shape failures remain distinct.
#[derive(Debug)]
pub enum LayoutCreationFailure<H> {
    Layout(LayoutCompileError),
    Creator(H),
    Resource(novarocks_type_contract::owned_resources::layout::LayoutResourceError),
}
impl<H> From<LayoutCompileError> for LayoutCreationFailure<H> {
    fn from(error: LayoutCompileError) -> Self {
        Self::Layout(error)
    }
}
impl<H> From<LayoutError> for LayoutCreationFailure<H> {
    fn from(error: LayoutError) -> Self {
        Self::Layout(error.into())
    }
}
impl<H> From<CompileControlError> for LayoutCreationFailure<H> {
    fn from(error: CompileControlError) -> Self {
        Self::Layout(error.into())
    }
}
impl<H> From<novarocks_type_contract::owned_resources::layout::LayoutResourceError>
    for LayoutCreationFailure<H>
{
    fn from(error: novarocks_type_contract::owned_resources::layout::LayoutResourceError) -> Self {
        Self::Resource(error)
    }
}
fn compile_with_creator<T, H>(
    control: &dyn PureCompileControl,
    operation: impl FnOnce(&mut LayoutWork<'_>) -> Result<T, LayoutCreationFailure<H>>,
) -> Result<T, LayoutCreationFailure<H>> {
    let mut work = LayoutWork(Some(CompileCheckpoints::try_new(
        control,
        CompilePhase::LowerProgram,
    )?));
    let result = operation(&mut work);
    if matches!(
        &result,
        Err(LayoutCreationFailure::Creator(_)
            | LayoutCreationFailure::Layout(LayoutCompileError::Control(_)))
    ) {
        return result;
    }
    work.flush()?;
    result
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
    /// Production lowering uses its actual typed layout creator.
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
            slot_index: None,
            typed_representation: false,
            storage: ValueTypeStorageLifetime::pure(),
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
    pub fn slot_metadata_at(&self, index: usize) -> Option<(&ValueTypeRef, Option<i32>)> {
        let metadata = self.slot_metadata.as_ref()?.get(index)?;
        Some((&metadata.field_schema, metadata.unique_id))
    }
    pub fn value_binding(&self, slot: SlotId) -> Option<&TypedValueBinding> {
        let index = if let Some(index) = &self.slot_index {
            index[index.binary_search_by_key(&slot, |entry| entry.0).ok()?].1
        } else {
            self.slots.iter().position(|actual| *actual == slot)?
        };
        self.slot_metadata.as_ref()?.get(index)?.binding.as_ref()
    }
    pub fn value_binding_at(&self, index: usize) -> Option<&TypedValueBinding> {
        self.slot_metadata.as_ref()?.get(index)?.binding.as_ref()
    }
    pub fn is_typed(&self) -> bool {
        self.typed_representation
    }

    /// Validate an exchange projection against the already authored layout.
    /// This borrows both bindings and allocates no projection scaffolds.
    pub fn typed_projection_matches_for_compile(
        &self,
        output_columns: &[SlotId],
        expected: &Self,
        control: &dyn PureCompileControl,
    ) -> Result<bool, LayoutCompileError> {
        compile(control, |work| {
            self.typed_projection_matches_inner(output_columns, expected, false, work)
        })
    }

    pub fn typed_projection_matches(
        &self,
        output_columns: &[SlotId],
        expected: &Self,
    ) -> Result<bool, LayoutError> {
        legacy(|work| self.typed_projection_matches_inner(output_columns, expected, false, work))
    }

    /// Writer execution values and provider row obligations have independent
    /// root NULL guarantees. Every other slot/name/type/layout fact is exact.
    pub(crate) fn writer_projection_matches_for_compile(
        &self,
        expected: &Self,
        control: &dyn PureCompileControl,
    ) -> Result<bool, LayoutCompileError> {
        compile(control, |work| {
            self.typed_projection_matches_inner(&[], expected, true, work)
        })
    }

    pub(crate) fn writer_projection_matches(&self, expected: &Self) -> Result<bool, LayoutError> {
        legacy(|work| self.typed_projection_matches_inner(&[], expected, true, work))
    }

    fn typed_projection_matches_inner(
        &self,
        output_columns: &[SlotId],
        expected: &Self,
        writer_row_obligation: bool,
        work: &mut LayoutWork<'_>,
    ) -> Result<bool, LayoutCompileError> {
        work.step()?;
        if !self.is_typed() || !expected.is_typed() {
            return Err(LayoutError::ValueBindingMismatch.into());
        }
        if writer_row_obligation
            && (self.schema.metadata().as_arc().is_some()
                || expected.schema.metadata().as_arc().is_some())
        {
            return Err(LayoutError::ValueBindingMismatch.into());
        }
        let columns = if output_columns.is_empty() {
            self.slots()
        } else {
            output_columns
        };
        if columns.len() != expected.slots().len() {
            return Ok(false);
        }
        let index = self
            .slot_index
            .as_ref()
            .ok_or(LayoutError::ValueBindingMismatch)?;
        for (ordinal, slot) in columns.iter().enumerate() {
            work.step()?;
            if *slot != expected.slots()[ordinal] {
                return Ok(false);
            }
            let mut lo = 0;
            let mut hi = index.len();
            while lo < hi {
                work.step()?;
                let mid = lo + (hi - lo) / 2;
                if index[mid].0 < *slot {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            let source_ordinal = index
                .get(lo)
                .filter(|entry| entry.0 == *slot)
                .ok_or(LayoutError::UnknownSlot)?
                .1;
            let source_field = &self.schema.fields()[source_ordinal];
            let target_field = &expected.schema.fields()[ordinal];
            work.step()?;
            if source_field.name().len() != target_field.name().len()
                || (!writer_row_obligation
                    && source_field.is_nullable() != target_field.is_nullable())
                || (writer_row_obligation
                    && (source_field.metadata().as_arc().is_some()
                        || target_field.metadata().as_arc().is_some()))
            {
                return Ok(false);
            }
            for (a, b) in source_field
                .name()
                .as_bytes()
                .chunks(MAX_UNOBSERVED_COMPILE_WORK as usize)
                .zip(
                    target_field
                        .name()
                        .as_bytes()
                        .chunks(MAX_UNOBSERVED_COMPILE_WORK as usize),
                )
            {
                for _ in a {
                    work.step()?;
                }
                if a != b {
                    return Ok(false);
                }
            }
            let source = self
                .slot_metadata
                .as_ref()
                .ok_or(LayoutError::ValueBindingMismatch)?;
            let target = expected
                .slot_metadata
                .as_ref()
                .ok_or(LayoutError::ValueBindingMismatch)?;
            let source = &source[source_ordinal];
            let target = &target[ordinal];
            work.step()?;
            if source.unique_id != target.unique_id {
                return Ok(false);
            }
            if writer_row_obligation {
                let source = source
                    .binding
                    .as_ref()
                    .ok_or(LayoutError::ValueBindingMismatch)?;
                let target = target
                    .binding
                    .as_ref()
                    .ok_or(LayoutError::ValueBindingMismatch)?;
                if !source
                    .borrowed()
                    .same_physical_binding_ignoring_root_nullability_observed(
                        target.borrowed(),
                        || work.step(),
                    )?
                {
                    return Ok(false);
                }
            } else {
                let mut a = Sha256::new();
                let mut b = Sha256::new();
                hash_field_schema(&mut a, &source.field_schema, work)?;
                hash_field_schema(&mut b, &target.field_schema, work)?;
                if a.finalize() != b.finalize() {
                    return Ok(false);
                }
            }
            let binding = source
                .binding
                .as_ref()
                .ok_or(LayoutError::ValueBindingMismatch)?;
            if !binding
                .layout()
                .matches_projection_observed(target_field.data_type(), |units| {
                    for _ in 0..units {
                        work.step()?;
                    }
                    Ok::<_, LayoutCompileError>(())
                })?
            {
                return Ok(false);
            }
        }
        Ok(true)
    }
    pub fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
        self.storage.clone()
    }

    /// Bind the actual static Arrow projection to its explicit immutable value
    /// types. No domain is reconstructed from annotations, and independently
    /// retained bindings keep their original creation responsibility.
    pub fn try_new_typed_for_compile(
        schema: SchemaRef,
        slots: Arc<[SlotId]>,
        bindings: Vec<(TypedValueBinding, Option<i32>)>,
        storage: ValueTypeStorageLifetime,
        mut request: impl FnMut(std::alloc::Layout) -> Result<(), LayoutCompileError>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, LayoutCompileError> {
        match Self::try_new_typed_with_creator::<std::convert::Infallible>(
            schema,
            slots,
            bindings,
            storage,
            |layout| request(layout).map_err(Into::into),
            control,
        ) {
            Ok(value) => Ok(value),
            Err(LayoutCreationFailure::Layout(error)) => Err(error),
            Err(LayoutCreationFailure::Creator(never)) => match never {},
            Err(LayoutCreationFailure::Resource(error)) => Err(error.into()),
        }
    }

    /// Exact element-array geometry used by the typed slot metadata creator.
    /// The creator requests this Vec backing and its independent Arc backing.
    pub fn typed_slot_metadata_allocation_layout(
        count: usize,
    ) -> Result<
        std::alloc::Layout,
        novarocks_type_contract::owned_resources::layout::LayoutResourceError,
    > {
        std::alloc::Layout::array::<StaticSlotMetadata>(count).map_err(|_| {
            novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic
        })
    }

    pub fn try_new_typed_with_creator<H>(
        schema: SchemaRef,
        slots: Arc<[SlotId]>,
        bindings: Vec<(TypedValueBinding, Option<i32>)>,
        storage: ValueTypeStorageLifetime,
        mut request: impl FnMut(std::alloc::Layout) -> Result<(), LayoutCreationFailure<H>>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, LayoutCreationFailure<H>> {
        compile_with_creator(control, |work| {
            work.step()?;
            if schema.fields().len() != slots.len() || bindings.len() != slots.len() {
                return Err(LayoutError::ArityMismatch.into());
            }
            if schema.metadata().as_arc().is_some() {
                return Err(LayoutError::ValueBindingMismatch.into());
            }
            let array_layout = std::alloc::Layout::array::<(SlotId, usize)>(slots.len())
                .map_err(|_| LayoutCreationFailure::Resource(novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic))?;
            if !slots.is_empty() {
                request(array_layout)?;
            }
            let mut ordered = Vec::with_capacity(slots.len());
            for (index, slot) in slots.iter().enumerate() {
                work.step()?;
                ordered.push((*slot, index));
            }
            sort_slots_observed(&mut ordered, work)?;
            for pair in ordered.windows(2) {
                work.step()?;
                if pair[0].0 == pair[1].0 {
                    return Err(LayoutError::DuplicateSlot.into());
                }
            }
            let metadata_layout = Self::typed_slot_metadata_allocation_layout(slots.len())?;
            if !slots.is_empty() {
                request(metadata_layout)?;
            }
            let mut metadata = Vec::with_capacity(slots.len());
            let mut expanded = 0usize;
            for (field, (binding, unique_id)) in schema.fields().iter().zip(bindings) {
                work.step()?;
                if field.metadata().as_arc().is_some()
                    || field.is_nullable() != binding.nullable()
                    || !binding.layout().matches_projection_observed(
                        field.data_type(),
                        |units| {
                            for _ in 0..units {
                                work.step()?;
                            }
                            Ok::<_, LayoutCompileError>(())
                        },
                    )?
                {
                    return Err(LayoutError::ValueBindingMismatch.into());
                }
                expanded = expanded
                    .checked_add(binding.value_type().expanded_nodes())
                    .filter(|nodes| *nodes <= MAX_SLOT_METADATA_NODES)
                    .ok_or(LayoutError::TooManyMetadataNodes)?;
                work.step()?;
                metadata.push(StaticSlotMetadata {
                    field_schema: binding.value_type().clone(),
                    binding: Some(binding),
                    unique_id,
                });
            }
            request(
                novarocks_type_contract::owned_resources::layout::arc_layout(metadata_layout)?,
            )?;
            work.step()?;
            let slot_metadata = Some(Arc::from(metadata));
            request(novarocks_type_contract::owned_resources::layout::arc_layout(array_layout)?)?;
            work.step()?;
            let slot_index = Some(Arc::from(ordered));
            Ok(Self {
                schema,
                slots,
                slot_metadata,
                metadata_materializations: None,
                field_metadata_origins: None,
                schema_metadata_origin: None,
                slot_index,
                typed_representation: true,
                storage,
            })
        })
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

    /// Construct new static projection scaffolds under their actual owner while
    /// sharing original immutable fields/types and keeping only their storage
    /// lifetimes. Empty output retains the existing complete-layout convention.
    pub fn project_by_slots_typed_for_compile(
        &self,
        output_columns: &[SlotId],
        storage: ValueTypeStorageLifetime,
        mut request: impl FnMut(std::alloc::Layout) -> Result<(), LayoutCompileError>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, LayoutCompileError> {
        match self.project_by_slots_typed_with_creator::<std::convert::Infallible>(
            output_columns,
            storage,
            |layout| request(layout).map_err(Into::into),
            control,
        ) {
            Ok(value) => Ok(value),
            Err(LayoutCreationFailure::Layout(error)) => Err(error),
            Err(LayoutCreationFailure::Resource(error)) => Err(error.into()),
            Err(LayoutCreationFailure::Creator(never)) => match never {},
        }
    }

    pub fn project_by_slots_typed_with_creator<H>(
        &self,
        output_columns: &[SlotId],
        storage: ValueTypeStorageLifetime,
        mut request: impl FnMut(std::alloc::Layout) -> Result<(), LayoutCreationFailure<H>>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, LayoutCreationFailure<H>> {
        if !self.is_typed() {
            return Err(LayoutError::ValueBindingMismatch.into());
        }
        if output_columns.is_empty() {
            control.checkpoint(CompilePhase::LowerProgram, 0)?;
            return Ok(self.clone());
        }
        let (schema, slots, bindings, storage) = compile_with_creator(control, |work| {
            let storage = storage.retaining_observed(&self.storage, &mut request)?;
            let count = output_columns.len();
            request(layout_array::<arrow_schema::FieldRef>(count)?)?;
            request(layout_array::<SlotId>(count)?)?;
            request(layout_array::<(TypedValueBinding, Option<i32>)>(count)?)?;
            let mut fields = Vec::with_capacity(count);
            let mut slots = Vec::with_capacity(count);
            let mut bindings = Vec::with_capacity(count);
            for requested in output_columns {
                let index = self
                    .slot_index
                    .as_ref()
                    .ok_or(LayoutError::ValueBindingMismatch)?;
                let mut lo = 0;
                let mut hi = index.len();
                while lo < hi {
                    work.step()?;
                    let mid = lo + (hi - lo) / 2;
                    match index[mid].0.cmp(requested) {
                        Ordering::Less => lo = mid + 1,
                        _ => hi = mid,
                    }
                }
                work.step()?;
                let index = index
                    .get(lo)
                    .filter(|entry| entry.0 == *requested)
                    .ok_or(LayoutError::UnknownSlot)?
                    .1;
                let metadata = self
                    .slot_metadata
                    .as_ref()
                    .and_then(|values| values.get(index))
                    .ok_or(LayoutError::ValueBindingMismatch)?;
                let binding = metadata
                    .binding
                    .as_ref()
                    .ok_or(LayoutError::ValueBindingMismatch)?;
                work.step()?;
                fields.push(Arc::clone(&self.schema.fields()[index]));
                work.step()?;
                bindings.push((binding.clone_observed(|| work.step())?, metadata.unique_id));
                slots.push(*requested);
            }
            request(
                novarocks_type_contract::owned_resources::layout::arc_layout(layout_array::<
                    arrow_schema::FieldRef,
                >(
                    count
                )?)?,
            )?;
            request(
                novarocks_type_contract::owned_resources::layout::arc_layout(
                    std::alloc::Layout::new::<Schema>(),
                )?,
            )?;
            work.step()?;
            let schema = Arc::new(Schema::new(fields));
            request(
                novarocks_type_contract::owned_resources::layout::arc_layout(layout_array::<
                    SlotId,
                >(
                    count
                )?)?,
            )?;
            work.step()?;
            Ok((schema, Arc::from(slots), bindings, storage))
        })?;
        Self::try_new_typed_with_creator(schema, slots, bindings, storage, request, control)
    }
    fn project_inner(
        &self,
        output_columns: &[SlotId],
        work: &mut LayoutWork<'_>,
    ) -> Result<Self, LayoutCompileError> {
        if output_columns.is_empty() {
            return Ok(self.clone());
        }
        if self.is_typed() {
            return Err(LayoutError::TypedProjectionRequiresOwner.into());
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
                        .map_err(|_| LayoutError::MetadataOwnerConflict)
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
        let schema = work
            .opaque(|| serde_json::to_value(self.schema.as_ref()).map_err(LayoutError::encoding))?;
        let schema = canonicalize_json(schema, work)?;
        let schema = work.opaque(|| serde_json::to_vec(&schema).map_err(LayoutError::encoding))?;
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
fn layout_array<T>(count: usize) -> Result<std::alloc::Layout, LayoutCompileError> {
    std::alloc::Layout::array::<T>(count).map_err(|_| LayoutError::AllocationModel.into())
}

fn sort_slots_observed(
    slots: &mut [(SlotId, usize)],
    work: &mut LayoutWork<'_>,
) -> Result<(), LayoutCompileError> {
    fn sift(
        slots: &mut [(SlotId, usize)],
        mut root: usize,
        end: usize,
        work: &mut LayoutWork<'_>,
    ) -> Result<(), LayoutCompileError> {
        while root < end / 2 {
            let mut child = root * 2 + 1;
            work.step()?;
            if child + 1 < end {
                work.step()?;
                if slots[child].0 < slots[child + 1].0 {
                    child += 1;
                }
            }
            work.step()?;
            if slots[root].0 >= slots[child].0 {
                break;
            }
            work.step()?;
            slots.swap(root, child);
            root = child;
        }
        Ok(())
    }
    let len = slots.len();
    for root in (0..len / 2).rev() {
        sift(slots, root, len, work)?;
    }
    for end in (1..len).rev() {
        work.step()?;
        slots.swap(0, end);
        sift(slots, 0, end, work)?;
    }
    Ok(())
}

fn validate_metadata(
    metadata: &[StaticSlotMetadata],
    work: &mut LayoutWork<'_>,
) -> Result<(), LayoutCompileError> {
    // Each immutable value node already passed its per-value depth/node rules.
    // This port owns the expanded schema profile, including repeated roots.
    let mut count = 0usize;
    for slot in metadata {
        count = count
            .checked_add(slot.field_schema().expanded_nodes())
            .ok_or(LayoutError::TooManyMetadataNodes)?;
        work.step()?;
        if count > MAX_SLOT_METADATA_NODES {
            return Err(LayoutError::TooManyMetadataNodes.into());
        }
        if slot.field_schema().depth() > MAX_SLOT_METADATA_DEPTH {
            return Err(LayoutError::TooDeep.into());
        }
    }
    Ok(())
}
fn hash_field_schema(
    digest: &mut Sha256,
    root: &ValueTypeRef,
    work: &mut LayoutWork<'_>,
) -> Result<(), LayoutCompileError> {
    write_value_type_identity_observed(root, |bytes| work.hash_bytes(digest, bytes))
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
    use novarocks_type_contract::{ValueKind, ValueLogicalType, ValueTypeStorageLifetime};
    fn value(kind: ValueKind, nullable: bool) -> ValueTypeRef {
        ValueTypeRef::try_new(kind, nullable, ValueTypeStorageLifetime::pure()).unwrap()
    }
    use arrow_schema::{DataType, Field, Schema};
    fn complete(value: ValueTypeRef, carrier: &DataType) -> TypedValueBinding {
        TypedValueBinding::try_new(
            value,
            novarocks_type_contract::ValueLayout::try_new(
                carrier,
                ValueTypeStorageLifetime::pure(),
            )
            .unwrap(),
        )
        .unwrap()
    }
    // FE fixture declarations are pure. This callback records numerical
    // constructor contributions; it is not a BE creator or capacity grant.
    fn pure_request(
        bytes: &mut usize,
        layout: std::alloc::Layout,
    ) -> Result<(), LayoutCompileError> {
        *bytes = bytes
            .checked_add(layout.size())
            .ok_or(CompileControlError::ResourceExhausted)?;
        Ok(())
    }
    fn typed_fixture(
        schema: SchemaRef,
        slots: Arc<[SlotId]>,
        facts: Vec<(TypedValueBinding, Option<i32>)>,
        control: &dyn PureCompileControl,
    ) -> Result<StaticLayout, LayoutCompileError> {
        let mut requested = 0;
        StaticLayout::try_new_typed_for_compile(
            schema,
            slots,
            facts,
            ValueTypeStorageLifetime::pure(),
            |layout| pure_request(&mut requested, layout),
            control,
        )
    }
    fn typed_projection(
        source: &StaticLayout,
        slots: &[SlotId],
        control: &dyn PureCompileControl,
    ) -> Result<StaticLayout, LayoutCompileError> {
        let mut requested = 0;
        source.project_by_slots_typed_for_compile(
            slots,
            ValueTypeStorageLifetime::pure(),
            |layout| pure_request(&mut requested, layout),
            control,
        )
    }

    #[test]
    fn writer_projection_preserves_complete_channels_with_independent_root_null_obligations() {
        fn list(kind: ValueKind, child_nullable: bool, nullable: bool) -> TypedValueBinding {
            complete(
                value(ValueKind::List(value(kind, child_nullable)), nullable),
                &DataType::List(Arc::new(Field::new("item", DataType::Utf8, child_nullable))),
            )
        }
        fn declared(
            binding: TypedValueBinding,
            name: &str,
            slot: u32,
            unique: Option<i32>,
        ) -> StaticLayout {
            let schema = Arc::new(Schema::new(vec![Field::new(
                name,
                binding.data_type().clone(),
                binding.nullable(),
            )]));
            typed_fixture(
                schema,
                Arc::from([SlotId::new(slot)]),
                vec![(binding, unique)],
                &OriginalControl::default(),
            )
            .unwrap()
        }
        let execution = declared(
            list(ValueKind::Nominal(ValueLogicalType::Json), false, true),
            "document",
            1,
            Some(9),
        );
        let provider = declared(
            list(ValueKind::Nominal(ValueLogicalType::Json), false, false),
            "document",
            1,
            Some(9),
        );
        assert_ne!(
            execution.identity().unwrap(),
            provider.identity().unwrap(),
            "the exact public/exchange identity is unchanged"
        );
        assert!(
            !execution.typed_projection_matches(&[], &provider).unwrap(),
            "ordinary projected ports keep exact root NULL guarantees"
        );
        assert!(
            execution.writer_projection_matches(&provider).unwrap(),
            "the provider's NOT NULL remains a separate runtime row obligation"
        );
        assert!(provider.writer_projection_matches(&execution).unwrap());
        for wrong in [
            declared(
                list(ValueKind::String, false, false),
                "document",
                1,
                Some(9),
            ),
            declared(
                list(ValueKind::Nominal(ValueLogicalType::Json), true, false),
                "document",
                1,
                Some(9),
            ),
            declared(
                list(ValueKind::Nominal(ValueLogicalType::Json), false, false),
                "renamed",
                1,
                Some(9),
            ),
            declared(
                list(ValueKind::Nominal(ValueLogicalType::Json), false, false),
                "document",
                2,
                Some(9),
            ),
            declared(
                list(ValueKind::Nominal(ValueLogicalType::Json), false, false),
                "document",
                1,
                Some(10),
            ),
        ] {
            assert!(
                !execution.writer_projection_matches(&wrong).unwrap(),
                "logical domain, child NULL, names, slot identity and unique identity remain exact"
            );
        }
        let control = OriginalControl::default();
        assert!(
            execution
                .writer_projection_matches_for_compile(&provider, &control)
                .unwrap()
        );
        let trace = control.trace.lock().unwrap().clone();
        assert!(!trace.is_empty());
        for cause in causes() {
            for prefix in 0..trace.len() {
                let stopped = OriginalControl {
                    stop: Some((prefix, cause.clone())),
                    ..OriginalControl::default()
                };
                assert_eq!(
                    execution.writer_projection_matches_for_compile(&provider, &stopped),
                    Err(LayoutCompileError::Control(cause.clone()))
                );
                assert_eq!(
                    stopped.trace.lock().unwrap().as_slice(),
                    &trace[..=prefix],
                    "the original first control cause has no trailing comparison/footer"
                );
            }
        }
    }

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
    fn identity_distinguishes_complete_domains_with_the_same_carrier() {
        let layout = |kind| {
            let carrier = DataType::FixedSizeBinary(16);
            typed_fixture(
                Arc::new(Schema::new(vec![Field::new(
                    "value",
                    carrier.clone(),
                    true,
                )])),
                Arc::from([SlotId::new(1)]),
                vec![(complete(value(kind, true), &carrier), None)],
                &OriginalControl::default(),
            )
            .unwrap()
        };
        let physical = layout(ValueKind::Scalar(DataType::FixedSizeBinary(16)))
            .identity()
            .unwrap();
        let large = layout(ValueKind::Nominal(ValueLogicalType::LargeInt))
            .identity()
            .unwrap();
        let uuid = layout(ValueKind::Nominal(ValueLogicalType::Uuid))
            .identity()
            .unwrap();
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
        let slots: Arc<[SlotId]> = Arc::from([SlotId::new(7)]);
        let unspecified = StaticLayout::try_new(schema.clone(), slots.clone()).unwrap();
        let control = OriginalControl::default();
        let plain = typed_fixture(
            schema.clone(),
            slots.clone(),
            vec![(
                complete(value(ValueKind::String, true), &DataType::Utf8),
                None,
            )],
            &control,
        )
        .unwrap();
        let logical = typed_fixture(
            schema.clone(),
            slots.clone(),
            vec![(
                complete(
                    value(ValueKind::Nominal(ValueLogicalType::Json), true),
                    &DataType::Utf8,
                ),
                None,
            )],
            &control,
        )
        .unwrap();
        let unique = typed_fixture(
            schema.clone(),
            slots.clone(),
            vec![(
                complete(value(ValueKind::String, true), &DataType::Utf8),
                Some(3),
            )],
            &control,
        )
        .unwrap();
        assert!(!unspecified.has_exact_slot_metadata());
        assert!(plain.has_exact_slot_metadata());
        assert!(plain.is_typed() && logical.is_typed() && unique.is_typed());
        assert_ne!(unspecified.identity().unwrap(), plain.identity().unwrap());
        assert_ne!(plain.identity().unwrap(), logical.identity().unwrap());
        assert_ne!(plain.identity().unwrap(), unique.identity().unwrap());
        assert_eq!(
            logical.slot_metadata_at(0).unwrap().0.domain(),
            Some(ValueLogicalType::Json)
        );
        assert_eq!(unique.slot_metadata_at(0).unwrap().1, Some(3));
        assert!(Arc::ptr_eq(plain.schema(), &schema));
        assert!(Arc::ptr_eq(logical.schema(), &schema));
    }

    #[test]
    fn projection_keeps_exact_nested_slot_metadata() {
        let original_source = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, true),
        ]));
        let nested = value(
            ValueKind::List(value(ValueKind::Nominal(ValueLogicalType::Bitmap), true)),
            true,
        );
        let nested_carrier = DataType::List(Arc::new(Field::new("item", DataType::Binary, true)));
        let facts = vec![
            (
                complete(value(ValueKind::String, true), &DataType::Utf8),
                None,
            ),
            (complete(nested.clone(), &nested_carrier), Some(9)),
        ];
        let control = OriginalControl::default();
        let slots: Arc<[SlotId]> = Arc::from([SlotId::new(1), SlotId::new(2)]);
        // The old source carrier is preserved independently. It cannot stand
        // in for these explicit String/List<Bitmap> execution declarations.
        assert!(matches!(
            typed_fixture(
                original_source.clone(),
                slots.clone(),
                facts.clone(),
                &control
            ),
            Err(LayoutCompileError::Layout(
                LayoutError::ValueBindingMismatch
            ))
        ));
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", nested_carrier, true),
        ]));
        let source = typed_fixture(schema, slots, facts, &control).unwrap();
        let projected = typed_projection(&source, &[SlotId::new(2)], &control).unwrap();
        assert_eq!(projected.slot_metadata_at(0), Some((&nested, Some(9))));
        assert_ne!(projected.identity().unwrap(), source.identity().unwrap());
        assert_eq!(
            typed_projection(&source, &[], &control).unwrap().identity(),
            source.identity()
        );
        assert_eq!(original_source.field(0).data_type(), &DataType::Int64);
        assert!(!original_source.field(0).is_nullable());
        assert_eq!(original_source.field(1).data_type(), &DataType::Utf8);
        assert!(original_source.field(1).is_nullable());
    }

    #[test]
    fn rejects_unbounded_expanded_schema_profile() {
        let mut item = value(ValueKind::String, true);
        let mut carrier = DataType::Utf8;
        for _ in 1..MAX_SLOT_METADATA_DEPTH {
            item = value(ValueKind::List(item), true);
            carrier = DataType::List(Arc::new(Field::new("item", carrier, true)));
        }
        let count = MAX_SLOT_METADATA_NODES / MAX_SLOT_METADATA_DEPTH + 1;
        let binding = complete(item, &carrier);
        let roots = vec![(binding, None); count];
        let original_source = Arc::new(Schema::new(
            (0..count)
                .map(|i| Field::new(format!("v{i}"), DataType::Utf8, true))
                .collect::<Vec<_>>(),
        ));
        let slots: Arc<[SlotId]> = Arc::from(
            (0..count)
                .map(|i| SlotId::new(i as u32))
                .collect::<Vec<_>>(),
        );
        let control = OriginalControl::default();
        assert!(matches!(
            typed_fixture(
                original_source.clone(),
                slots.clone(),
                roots.clone(),
                &control
            ),
            Err(LayoutCompileError::Layout(
                LayoutError::ValueBindingMismatch
            ))
        ));
        let schema = Arc::new(Schema::new(
            (0..count)
                .map(|i| Field::new(format!("v{i}"), carrier.clone(), true))
                .collect::<Vec<_>>(),
        ));
        let result = typed_fixture(schema, slots, roots, &control);
        assert!(matches!(
            result,
            Err(LayoutCompileError::Layout(
                LayoutError::TooManyMetadataNodes
            ))
        ));
        assert_eq!(original_source.fields().len(), count);
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
            if let Some((at, cause)) = self.stop.clone()
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
        let original_source = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("a", DataType::Int64, false),
                Field::new("b", DataType::Utf8, true).with_metadata(metadata.clone()),
            ],
            metadata.clone(),
        ));
        let slots: Arc<[SlotId]> = Arc::from([SlotId::new(9), SlotId::new(3)]);
        let nested = value(
            ValueKind::List(value(ValueKind::Nominal(ValueLogicalType::Json), true)),
            true,
        );
        let carrier = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
        let facts = vec![
            (
                complete(value(ValueKind::String, true), &DataType::Utf8),
                None,
            ),
            (complete(nested.clone(), &carrier), Some(17)),
        ];
        let control = OriginalControl::default();
        assert!(matches!(
            typed_fixture(
                original_source.clone(),
                slots.clone(),
                facts.clone(),
                &control
            ),
            Err(LayoutCompileError::Layout(
                LayoutError::ValueBindingMismatch
            ))
        ));
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", carrier, true),
        ]));
        let legacy = typed_fixture(
            schema.clone(),
            slots.clone(),
            facts.clone(),
            &OriginalControl::default(),
        )
        .unwrap();
        let checked = typed_fixture(schema.clone(), slots.clone(), facts, &control).unwrap();
        assert!(Arc::ptr_eq(checked.schema(), &schema));
        assert_eq!(
            checked.identity_for_compile(&control).unwrap(),
            legacy.identity().unwrap()
        );
        let projected = typed_projection(&checked, &[SlotId::new(3)], &control).unwrap();
        assert_eq!(projected.slots(), &[SlotId::new(3)]);
        assert_eq!(projected.schema().field(0), schema.field(1));
        assert_eq!(projected.schema().metadata(), schema.metadata());
        assert!(projected.schema().metadata().as_arc().is_none());
        assert!(projected.schema().field(0).metadata().as_arc().is_none());
        assert_eq!(projected.slot_metadata_at(0), Some((&nested, Some(17))));
        assert_eq!(
            projected.identity_for_compile(&control).unwrap(),
            typed_projection(&legacy, &[SlotId::new(3)], &control)
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
        assert_eq!(original_source.field(0).data_type(), &DataType::Int64);
        assert!(!original_source.field(0).is_nullable());
        assert_eq!(original_source.field(1).metadata(), &metadata);
        assert_eq!(original_source.metadata(), &metadata);
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
                    stop: Some((0, cause.clone())),
                };
                let result = match entry {
                    0 => StaticLayout::try_new_for_compile(schema.clone(), slots.clone(), &control)
                        .map(|_| ()),
                    1 => typed_fixture(
                        schema.clone(),
                        slots.clone(),
                        vec![(
                            complete(
                                value(ValueKind::Scalar(DataType::Int64), false),
                                &DataType::Int64,
                            ),
                            None,
                        )],
                        &control,
                    )
                    .map(|_| ()),
                    2 => layout
                        .project_by_slots_for_compile(&[], &control)
                        .map(|_| ()),
                    _ => layout.identity_for_compile(&control).map(|_| ()),
                };
                assert_eq!(result, Err(LayoutCompileError::Control(cause.clone())));
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
                    stop: Some((at, cause.clone())),
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
                stop: Some((1, cause.clone())),
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
                stop: Some((trace.len() - 1, cause.clone())),
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
                stop: Some((1, cause.clone())),
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
                    stop: Some((at, cause.clone())),
                };
                assert_eq!(
                    layout.identity_for_compile(&control),
                    Err(LayoutCompileError::Control(cause.clone()))
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
                stop: Some((quantum, cause.clone())),
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

#[cfg(test)]
mod typed_tests;
