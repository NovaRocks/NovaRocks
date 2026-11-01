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
use super::construction_failure::{
    ChunkConstructionFailure, ChunkObservationFailure, ChunkSchemaViolation,
};
use super::construction_failure::{SchemaContractSite, schema_contract, schema_type_mismatch};
use std::collections::HashMap;
use std::sync::Arc;

use super::typed_array_normalization::{
    ArrayNormalizationStep, execution_field_is_normalized, execution_field_is_normalized_observed,
    normalize_execution_field_observed,
};
use crate::exec::chunk::type_compatibility::check_exact;
use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use novarocks_type_contract::owned_resources::metadata_materialization::{
    MaterializedMetadataMap, MetadataFieldLoan, OriginalFieldMaterialization,
    OriginalMetadataClone, OriginalSharedField, SchemaMetadataMaterializations,
};
use novarocks_type_contract::{
    TypedValueBinding, ValueField, ValueKind, ValueLayout, ValueLogicalType, ValueTypeRef,
    ValueTypeStorageLifetime, ValueUnionField,
};
use novarocks_types::SlotId;
use novarocks_types::arrow_metadata_owner::{
    ArrowMetadataOwner, FieldMetadataOrigins, MetadataOwnedSchema, MetadataOwnerLimits,
};

/// A borrowed view of the unique immutable semantic type authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChunkFieldSchema {
    value_type: ValueTypeRef,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ChunkFieldSchemaCloneRequest {
    pub allocation_requests: usize,
    pub request_bytes: usize,
}

impl ChunkFieldSchema {
    pub fn from_value_type(value_type: ValueTypeRef) -> Self {
        Self { value_type }
    }

    pub fn value_type(&self) -> &ValueTypeRef {
        &self.value_type
    }

    pub fn kind(&self) -> &ValueKind {
        self.value_type.kind()
    }

    /// Explicit external import retained only for the expansion adapter.
    /// Execution type authors must supply `from_value_type` instead.
    pub fn from_field(field: &Field) -> Result<Self, ChunkConstructionFailure> {
        ValueTypeRef::try_from_external_field(field, ValueTypeStorageLifetime::pure())
            .map(Self::from_value_type)
            .map_err(ChunkConstructionFailure::from)
    }

    pub fn logical_type(&self) -> Option<ValueLogicalType> {
        self.value_type.domain()
    }

    pub fn json_semantic(&self) -> bool {
        self.logical_type() == Some(ValueLogicalType::Json)
    }

    pub fn struct_child(&self, index: usize) -> Option<&ValueTypeRef> {
        match self.kind() {
            ValueKind::Struct(fields) => fields.get(index).map(|field| field.value_type()),
            _ => None,
        }
    }

    pub fn list_item(&self) -> Option<&ValueTypeRef> {
        match self.kind() {
            ValueKind::List(item) | ValueKind::FixedSizeList { item, .. } => Some(item),
            _ => None,
        }
    }

    pub fn map_key(&self) -> Option<&ValueTypeRef> {
        match self.kind() {
            ValueKind::Map { key, .. } => Some(key),
            _ => None,
        }
    }

    pub fn map_value(&self) -> Option<&ValueTypeRef> {
        match self.kind() {
            ValueKind::Map { value, .. } => Some(value),
            _ => None,
        }
    }

    #[cfg(test)]
    pub(crate) fn original_clone_request_observed<
        E: From<
            novarocks_type_contract::owned_resources::metadata_materialization::MetadataMaterializationError,
        >,
    >(
        &self,
        observe: &mut impl FnMut() -> Result<(), E>,
    ) -> Result<ChunkFieldSchemaCloneRequest, E>{
        observe()?;
        Ok(ChunkFieldSchemaCloneRequest::default())
    }

    #[cfg(test)]
    pub(crate) fn original_clone_allocation_requests_observed<
        E: From<
            novarocks_type_contract::owned_resources::metadata_materialization::MetadataMaterializationError,
        >,
    >(
        &self,
        observe: &mut impl FnMut() -> Result<(), E>,
        _allocations: &mut Option<
            novarocks_type_contract::owned_resources::metadata_materialization::MetadataAllocationLoan<'_, E>,
        >,
    ) -> Result<ChunkFieldSchemaCloneRequest, E>{
        self.original_clone_request_observed(observe)
    }
}

pub struct ChunkSlotSchema {
    slot_id: SlotId,
    field: OriginalSharedField,
    metadata_origins: Option<FieldMetadataOrigins>,
    field_schema: ChunkFieldSchema,
    typed_binding: TypedValueBinding,
    typed_representation: bool,
    storage: ValueTypeStorageLifetime,
    unique_id: Option<i32>,
}

impl std::fmt::Debug for ChunkSlotSchema {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        out.debug_struct("ChunkSlotSchema")
            .field("slot_id", &self.slot_id)
            .field("field", self.field.field_ref())
            .field("metadata_origins", &self.metadata_origins)
            .field("field_schema", &self.field_schema)
            .field("unique_id", &self.unique_id)
            .finish()
    }
}

impl Clone for ChunkSlotSchema {
    fn clone(&self) -> Self {
        Self {
            slot_id: self.slot_id,
            field: if self.field.shares_original_reference() {
                self.field.clone()
            } else {
                OriginalSharedField::from_original(self.field.clone_original())
            },
            metadata_origins: self.metadata_origins.clone(),
            field_schema: self.field_schema.clone(),
            typed_binding: self.typed_binding.clone(),
            typed_representation: self.typed_representation,
            storage: self.storage.clone(),
            unique_id: self.unique_id,
        }
    }
}

impl PartialEq for ChunkSlotSchema {
    fn eq(&self, other: &Self) -> bool {
        self.slot_id == other.slot_id
            && self.field == other.field
            && self.typed_binding == other.typed_binding
            && self.unique_id == other.unique_id
    }
}
impl Eq for ChunkSlotSchema {}

impl ChunkSlotSchema {
    fn legacy_semantics(
        field: &Field,
        supplied: Option<ChunkFieldSchema>,
    ) -> Result<ChunkFieldSchema, ChunkConstructionFailure> {
        let schema = match supplied {
            Some(schema) => schema,
            None => ChunkFieldSchema::from_field(field)?,
        };
        legacy_retarget_nullable(schema.value_type(), field.data_type(), field.is_nullable())
            .map(ChunkFieldSchema::from_value_type)
    }

    fn bind_carrier(
        schema: &ChunkFieldSchema,
        field: &Field,
        storage: ValueTypeStorageLifetime,
    ) -> Result<TypedValueBinding, ChunkConstructionFailure> {
        let layout = ValueLayout::try_new(field.data_type(), storage)
            .map_err(ChunkConstructionFailure::from)?;
        TypedValueBinding::try_new(schema.value_type().clone(), layout)
            .map_err(ChunkConstructionFailure::from)
    }

    /// Bind explicit semantics to a metadata-free actual field representation.
    /// The caller owns capacity for this construction and supplies its lifetime.
    pub fn try_new_typed(
        slot_id: SlotId,
        field: FieldRef,
        binding: TypedValueBinding,
        unique_id: Option<i32>,
        storage: ValueTypeStorageLifetime,
    ) -> Result<Self, ChunkConstructionFailure> {
        Self::try_new_typed_observed(slot_id, field, binding, unique_id, storage, |_| Ok(()))
    }

    pub fn try_new_typed_observed<E, F>(
        slot_id: SlotId,
        field: FieldRef,
        binding: TypedValueBinding,
        unique_id: Option<i32>,
        storage: ValueTypeStorageLifetime,
        mut observe: F,
    ) -> Result<Self, E>
    where
        E: From<ChunkConstructionFailure>,
        F: for<'a> FnMut(ArrayNormalizationStep<'a>) -> Result<(), E>,
    {
        validate_chunk_carrier_encoding_observed(field.data_type(), &mut |carrier| {
            observe(ArrayNormalizationStep::InspectType(carrier))
        })?;
        let field = normalize_execution_field_observed(&field, |step| {
            observe(step).map_err(ChunkObservationFailure::Caller)
        })
        .map_err(ChunkObservationFailure::into_caller)?;
        if field.is_nullable() != binding.nullable()
            || !execution_carrier_matches_layout_observed(
                binding.data_type(),
                field.data_type(),
                &mut |units| observe(ArrayNormalizationStep::InspectConstructionWork { units }),
            )?
        {
            return Err(E::from(ChunkConstructionFailure::contract(
                ChunkSchemaViolation::FieldBinding,
            )));
        }
        Ok(Self {
            slot_id,
            field: OriginalSharedField::from_shared(field, None),
            metadata_origins: None,
            field_schema: ChunkFieldSchema::from_value_type(binding.value_type().clone()),
            typed_binding: binding,
            typed_representation: true,
            storage,
            unique_id,
        })
    }

    pub fn typed_binding(&self) -> &TypedValueBinding {
        &self.typed_binding
    }

    pub fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
        self.storage.clone()
    }

    pub fn is_typed(&self) -> bool {
        self.typed_representation
    }

    /// An explicit root-nullability author operation with new storage supplied.
    pub fn with_nullable_typed(
        &self,
        nullable: bool,
        storage: ValueTypeStorageLifetime,
    ) -> Result<Self, ChunkConstructionFailure> {
        if self.nullable() == nullable {
            return Ok(self.clone());
        }
        let binding = self
            .typed_binding
            .with_nullable(nullable, storage.clone())
            .map_err(ChunkConstructionFailure::from)?;
        let field = Arc::new(self.field().clone().with_nullable(nullable));
        Self::try_new_typed(self.slot_id, field, binding, self.unique_id, storage)
    }

    pub fn with_field_typed(
        &self,
        field: FieldRef,
        storage: ValueTypeStorageLifetime,
    ) -> Result<Self, ChunkConstructionFailure> {
        Self::try_new_typed(
            self.slot_id,
            field,
            self.typed_binding.clone(),
            self.unique_id,
            storage,
        )
    }

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
    ) -> Result<Self, ChunkConstructionFailure> {
        Self::try_new_owned_field(
            slot_id,
            OriginalFieldMaterialization::Plain(field),
            field_schema,
            unique_id,
        )
    }

    fn try_new_owned_field(
        slot_id: SlotId,
        field: OriginalFieldMaterialization,
        field_schema: Option<ChunkFieldSchema>,
        unique_id: Option<i32>,
    ) -> Result<Self, ChunkConstructionFailure> {
        let field_schema = Self::legacy_semantics(field.field(), field_schema)?;
        let typed_binding = Self::bind_carrier(
            &field_schema,
            field.field(),
            ValueTypeStorageLifetime::pure(),
        )?;
        Ok(Self {
            slot_id,
            field_schema,
            typed_binding,
            typed_representation: false,
            storage: ValueTypeStorageLifetime::pure(),
            field: OriginalSharedField::from_original(field),
            metadata_origins: None,
            unique_id,
        })
    }

    /// Share a field without inferring metadata provenance from its values.
    pub fn try_new_with_field_ref(
        slot_id: SlotId,
        field: FieldRef,
        field_schema: Option<ChunkFieldSchema>,
        unique_id: Option<i32>,
    ) -> Result<Self, ChunkConstructionFailure> {
        let field_schema = Self::legacy_semantics(&field, field_schema)?;
        let typed_binding =
            Self::bind_carrier(&field_schema, &field, ValueTypeStorageLifetime::pure())?;
        Ok(Self {
            slot_id,
            field_schema,
            typed_binding,
            typed_representation: false,
            storage: ValueTypeStorageLifetime::pure(),
            field: OriginalSharedField::from_shared(field, None),
            metadata_origins: None,
            unique_id,
        })
    }

    pub fn try_new_with_metadata_origins(
        slot_id: SlotId,
        field: FieldRef,
        metadata_origins: FieldMetadataOrigins,
        field_schema: Option<ChunkFieldSchema>,
        unique_id: Option<i32>,
    ) -> Result<Self, ChunkConstructionFailure> {
        if metadata_origins.metadata_bytes_for(&field).is_none() {
            return Err(schema_contract(
                SchemaContractSite::Metadata,
                "chunk slot metadata receipt does not match the exact field owner",
            ));
        }
        let field_schema = Self::legacy_semantics(&field, field_schema)?;
        let typed_binding =
            Self::bind_carrier(&field_schema, &field, ValueTypeStorageLifetime::pure())?;
        Ok(Self {
            slot_id,
            field_schema,
            typed_binding,
            typed_representation: false,
            storage: ValueTypeStorageLifetime::pure(),
            field: OriginalSharedField::from_shared(field, None),
            metadata_origins: Some(metadata_origins),
            unique_id,
        })
    }

    pub fn metadata_origins(&self) -> Option<&FieldMetadataOrigins> {
        self.metadata_origins.as_ref()
    }

    pub fn field_ref(&self) -> &FieldRef {
        self.field.field_ref()
    }

    pub fn with_type_and_nullable(
        &self,
        data_type: DataType,
        nullable: bool,
    ) -> Result<Self, ChunkConstructionFailure> {
        if &data_type == self.field.field().data_type()
            && nullable == self.field.field().is_nullable()
        {
            return Ok(self.clone());
        }
        if self.is_typed() {
            return Err(schema_contract(
                SchemaContractSite::Mutation,
                "typed slot changes require an explicit replacement binding and construction lifetime",
            ));
        }
        if let Some(origins) = &self.metadata_origins {
            let original = origins.owner_for(self.field.field_ref()).ok_or_else(|| {
                schema_contract(
                    SchemaContractSite::Mutation,
                    "chunk field metadata origin is missing",
                )
            })?;
            let replacement = original
                .derive_field(
                    data_type,
                    nullable,
                    MetadataOwnerLimits {
                        entries: 65536,
                        construction_bytes: 96 * 1024 * 1024,
                    },
                )
                .map_err(ChunkConstructionFailure::from)?;
            let field = Arc::clone(replacement.field());
            let origins = origins
                .replacing_root(self.field.field_ref(), replacement, 65536)
                .map_err(ChunkConstructionFailure::from)?;
            if let Ok(scoped) = origins.for_field_tree(&field, 65536, 64) {
                Self::try_new_with_metadata_origins(
                    self.slot_id,
                    field,
                    scoped,
                    Some(self.field_schema.clone()),
                    self.unique_id,
                )
            } else {
                // A legal generic type replacement can introduce fields whose
                // metadata backing is unknown. Keep its semantics and exact
                // Field Arc, but never issue a complete source-origin claim.
                Self::try_new_with_field_ref(
                    self.slot_id,
                    field,
                    Some(self.field_schema.clone()),
                    self.unique_id,
                )
            }
        } else {
            Self::try_new_owned_field(
                self.slot_id,
                self.field
                    .clone_original()
                    .rebuild_original(data_type, nullable),
                Some(self.field_schema.clone()),
                self.unique_id,
            )
        }
    }

    pub(crate) fn reconcile_to_carrier(
        &self,
        actual: &DataType,
        nullable: bool,
    ) -> Result<Self, ChunkConstructionFailure> {
        let Some(origins) = &self.metadata_origins else {
            let field = reconcile_chunk_field_to_data_type(self.field(), actual, nullable)?;
            return self.with_type_and_nullable(field.data_type().clone(), field.is_nullable());
        };
        // Only known immutable source maps can be copied. The actual carrier's
        // metadata remains a separate, unproven owner; it is never normalized.
        let scoped = origins
            .for_field_tree(self.field.field_ref(), 65536, 64)
            .map_err(ChunkConstructionFailure::from)?;
        let mut derived = Vec::new();
        let field = reconcile_owned_field(
            self.field.field_ref(),
            actual,
            nullable,
            &scoped,
            &mut derived,
        )?;
        if Arc::ptr_eq(self.field.field_ref(), &field) {
            return Ok(self.clone());
        }
        let origins = FieldMetadataOrigins::try_new(derived, 65536)
            .map_err(ChunkConstructionFailure::from)?;
        Self::try_new_with_metadata_origins(
            self.slot_id,
            field,
            origins,
            Some(self.field_schema.clone()),
            self.unique_id,
        )
    }

    /// Return a copy of this slot schema with nullable set to the given value.
    pub fn with_nullable(&self, nullable: bool) -> Self {
        if self.nullable() == nullable {
            return self.clone();
        }
        if self.metadata_origins.is_some() {
            return self
                .with_type_and_nullable(self.data_type().clone(), nullable)
                .unwrap_or_else(|error| panic!("{error}"));
        }
        assert!(
            !self.is_typed(),
            "typed nullability changes require with_nullable_typed and a creation lifetime"
        );
        Self::try_new_owned_field(
            self.slot_id,
            self.field.clone_original().with_nullable_original(nullable),
            Some(self.field_schema.clone()),
            self.unique_id,
        )
        .unwrap_or_else(|error| panic!("{error}"))
    }

    pub fn from_field(
        slot_id: SlotId,
        field: &Field,
        unique_id: Option<i32>,
    ) -> Result<Self, ChunkConstructionFailure> {
        Self::try_new_with_field(slot_id, field.clone(), None, unique_id)
    }

    pub fn with_field(&self, field: Field) -> Result<Self, ChunkConstructionFailure> {
        if self.is_typed() {
            if self.field() == &field {
                return Ok(self.clone());
            }
            return Err(schema_contract(
                SchemaContractSite::Mutation,
                "typed field replacement requires with_field_typed and a creation lifetime",
            ));
        }
        Self::try_new_with_field(
            self.slot_id,
            field,
            Some(self.field_schema.clone()),
            self.unique_id,
        )
    }

    /// Rename a validated channel while sharing its complete immutable owner.
    /// Slot identity changes neither value semantics nor encoding facts.
    pub fn with_slot_id_typed_observed<E: From<ChunkConstructionFailure>>(
        &self,
        slot_id: SlotId,
        mut observe: impl FnMut() -> Result<(), E>,
    ) -> Result<Self, E> {
        observe()?;
        if !self.is_typed() {
            return Err(E::from(ChunkConstructionFailure::contract(
                ChunkSchemaViolation::SlotRename,
            )));
        }
        let mut slot = self.clone();
        slot.slot_id = slot_id;
        Ok(slot)
    }

    pub fn with_slot_id(&self, slot_id: SlotId) -> Result<Self, ChunkConstructionFailure> {
        if self.field.shares_original_reference() {
            let mut slot = self.clone();
            slot.slot_id = slot_id;
            return Ok(slot);
        }
        Self::try_new_owned_field(
            slot_id,
            self.field.clone_original(),
            Some(self.field_schema.clone()),
            self.unique_id,
        )
    }

    pub fn with_field_and_slot_id(
        &self,
        slot_id: SlotId,
        field: Field,
    ) -> Result<Self, ChunkConstructionFailure> {
        if self.is_typed() {
            return self.with_field(field)?.with_slot_id(slot_id);
        }
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
        self.field.field()
    }

    pub fn name(&self) -> &str {
        self.field.field().name()
    }

    pub fn nullable(&self) -> bool {
        self.field.field().is_nullable()
    }

    pub fn data_type(&self) -> &DataType {
        self.field.field().data_type()
    }

    pub fn unique_id(&self) -> Option<i32> {
        self.unique_id
    }

    pub fn field_schema(&self) -> &ChunkFieldSchema {
        &self.field_schema
    }
}

#[derive(Clone)]
pub struct ChunkSchema {
    slots: Vec<ChunkSlotSchema>,
    arrow_schema: SchemaRef,
    slot_ids: Vec<SlotId>,
    index_by_slot: HashMap<SlotId, usize>,
    metadata_materializations: Option<SchemaMetadataMaterializations>,
    field_metadata_origins: Option<FieldMetadataOrigins>,
    schema_metadata_origin: Option<MetadataOwnedSchema>,
    typed_representation: bool,
    storage: ValueTypeStorageLifetime,
}

pub type ChunkSchemaRef = Arc<ChunkSchema>;

impl novarocks_type_contract::ClientInputTypeBindings for ChunkSchema {
    fn binding_at(&self, ordinal: usize) -> Option<&TypedValueBinding> {
        self.slots
            .get(ordinal)
            .filter(|slot| slot.is_typed())
            .map(ChunkSlotSchema::typed_binding)
    }

    fn slot_at(&self, ordinal: usize) -> Option<u32> {
        self.slot_ids.get(ordinal).map(|slot| slot.0)
    }

    fn matches_schema(&self, schema: &SchemaRef) -> bool {
        Arc::ptr_eq(&self.arrow_schema, schema)
    }
}

fn request_array<T, E: From<ChunkConstructionFailure>>(
    count: usize,
    request: &mut impl FnMut(std::alloc::Layout) -> Result<(), E>,
) -> Result<(), E> {
    if count == 0 {
        return Ok(());
    }
    request(
        std::alloc::Layout::array::<T>(count).map_err(|source| {
            E::from(ChunkConstructionFailure::array_request::<T>(count, source))
        })?,
    )
}

pub(super) fn request_arc_payload<E: From<ChunkConstructionFailure>>(
    payload: std::alloc::Layout,
    request: &mut impl FnMut(std::alloc::Layout) -> Result<(), E>,
) -> Result<(), E> {
    request(
        novarocks_type_contract::owned_resources::layout::arc_layout(payload)
            .map_err(|error| E::from(ChunkConstructionFailure::from(error)))?,
    )
}

/// Retain reused backing without retaining its former whole Chunk or context.
/// A distinct composite holder is requested before its actual Arc allocation.
pub(super) fn retain_storage_observed<E: From<ChunkConstructionFailure>>(
    storage: ValueTypeStorageLifetime,
    original: &ValueTypeStorageLifetime,
    request: &mut impl FnMut(std::alloc::Layout) -> Result<(), E>,
) -> Result<ValueTypeStorageLifetime, E> {
    enum RetentionError<E> {
        Request(E),
        Model(novarocks_type_contract::owned_resources::layout::LayoutResourceError),
    }
    impl<E> From<novarocks_type_contract::owned_resources::layout::LayoutResourceError>
        for RetentionError<E>
    {
        fn from(
            error: novarocks_type_contract::owned_resources::layout::LayoutResourceError,
        ) -> Self {
            Self::Model(error)
        }
    }
    storage
        .retaining_observed(original, |layout| {
            request(layout).map_err(RetentionError::Request)
        })
        .map_err(|error| match error {
            RetentionError::Request(error) => error,
            RetentionError::Model(error) => E::from(ChunkConstructionFailure::from(error)),
        })
}

/// Expansion-only nullability author for the old reconciliation adapter.
/// Domain and named-field meaning always come from the supplied type authority;
/// this never imports annotations or supplies missing array semantics.
fn legacy_retarget_nullable(
    value: &ValueTypeRef,
    carrier: &DataType,
    nullable: bool,
) -> Result<ValueTypeRef, ChunkConstructionFailure> {
    let carrier = match carrier {
        DataType::Dictionary(_, values) => {
            return legacy_retarget_nullable(value, values, nullable);
        }
        DataType::RunEndEncoded(_, values) => {
            return legacy_retarget_nullable(value, values.data_type(), nullable);
        }
        carrier => carrier,
    };
    let storage = ValueTypeStorageLifetime::pure();
    let kind = match (value.kind(), carrier) {
        (
            ValueKind::List(item),
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::ListView(field)
            | DataType::LargeListView(field),
        ) => ValueKind::List(legacy_retarget_nullable(
            item,
            field.data_type(),
            field.is_nullable(),
        )?),
        (ValueKind::FixedSizeList { item, length }, DataType::FixedSizeList(field, _)) => {
            ValueKind::FixedSizeList {
                item: legacy_retarget_nullable(item, field.data_type(), field.is_nullable())?,
                length: *length,
            }
        }
        (ValueKind::Struct(fields), DataType::Struct(actual)) if fields.len() == actual.len() => {
            // Fast borrowed check prevents a new field/name tree on each batch.
            if value.nullable() == nullable
                && fields.iter().zip(actual).all(|(field, actual)| {
                    legacy_nullable_matches(
                        field.value_type(),
                        actual.data_type(),
                        actual.is_nullable(),
                    )
                })
            {
                return Ok(value.clone());
            }
            ValueKind::Struct(
                fields
                    .iter()
                    .zip(actual)
                    .map(|(field, actual)| {
                        Ok(ValueField::new(
                            field.name(),
                            legacy_retarget_nullable(
                                field.value_type(),
                                actual.data_type(),
                                actual.is_nullable(),
                            )?,
                            storage.clone(),
                        ))
                    })
                    .collect::<Result<Vec<_>, ChunkConstructionFailure>>()?
                    .into_boxed_slice(),
            )
        }
        (ValueKind::Map { key, value }, DataType::Map(entries, _)) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(schema_contract(
                    SchemaContractSite::Reconciliation,
                    "legacy nullable author Map entries are not a Struct",
                ));
            };
            if fields.len() != 2 {
                return Err(schema_contract(
                    SchemaContractSite::Reconciliation,
                    "legacy nullable author Map entries have invalid arity",
                ));
            }
            ValueKind::Map {
                key: legacy_retarget_nullable(key, fields[0].data_type(), fields[0].is_nullable())?,
                value: legacy_retarget_nullable(
                    value,
                    fields[1].data_type(),
                    fields[1].is_nullable(),
                )?,
            }
        }
        (ValueKind::Union(fields), DataType::Union(actual, _)) if fields.len() == actual.len() => {
            if value.nullable() == nullable
                && fields
                    .iter()
                    .zip(actual.iter())
                    .all(|(field, (_, actual))| {
                        legacy_nullable_matches(
                            field.field.value_type(),
                            actual.data_type(),
                            actual.is_nullable(),
                        )
                    })
            {
                return Ok(value.clone());
            }
            ValueKind::Union(
                fields
                    .iter()
                    .zip(actual.iter())
                    .map(|(field, (_, actual))| {
                        Ok(ValueUnionField {
                            type_id: field.type_id,
                            field: ValueField::new(
                                field.field.name(),
                                legacy_retarget_nullable(
                                    field.field.value_type(),
                                    actual.data_type(),
                                    actual.is_nullable(),
                                )?,
                                storage.clone(),
                            ),
                        })
                    })
                    .collect::<Result<Vec<_>, ChunkConstructionFailure>>()?
                    .into_boxed_slice(),
            )
        }
        _ => {
            return if value.nullable() == nullable {
                Ok(value.clone())
            } else {
                value
                    .with_nullable(nullable, storage)
                    .map_err(ChunkConstructionFailure::from)
            };
        }
    };
    if &kind == value.kind() && nullable == value.nullable() {
        Ok(value.clone())
    } else {
        ValueTypeRef::try_new(kind, nullable, storage).map_err(ChunkConstructionFailure::from)
    }
}

fn legacy_nullable_matches(value: &ValueTypeRef, carrier: &DataType, nullable: bool) -> bool {
    if value.nullable() != nullable {
        return false;
    }
    match (value.kind(), carrier) {
        (_, DataType::Dictionary(_, values)) => legacy_nullable_matches(value, values, nullable),
        (_, DataType::RunEndEncoded(_, values)) => {
            legacy_nullable_matches(value, values.data_type(), nullable)
        }
        (
            ValueKind::List(item) | ValueKind::FixedSizeList { item, .. },
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::ListView(field)
            | DataType::LargeListView(field)
            | DataType::FixedSizeList(field, _),
        ) => legacy_nullable_matches(item, field.data_type(), field.is_nullable()),
        (ValueKind::Struct(fields), DataType::Struct(actual)) => {
            fields.len() == actual.len()
                && fields.iter().zip(actual).all(|(field, actual)| {
                    legacy_nullable_matches(
                        field.value_type(),
                        actual.data_type(),
                        actual.is_nullable(),
                    )
                })
        }
        (ValueKind::Map { key, value }, DataType::Map(entries, _)) => match entries.data_type() {
            DataType::Struct(fields) if fields.len() == 2 => {
                legacy_nullable_matches(key, fields[0].data_type(), fields[0].is_nullable())
                    && legacy_nullable_matches(
                        value,
                        fields[1].data_type(),
                        fields[1].is_nullable(),
                    )
            }
            _ => false,
        },
        (ValueKind::Union(fields), DataType::Union(actual, _)) => {
            fields.len() == actual.len()
                && fields
                    .iter()
                    .zip(actual.iter())
                    .all(|(field, (_, actual))| {
                        legacy_nullable_matches(
                            field.field.value_type(),
                            actual.data_type(),
                            actual.is_nullable(),
                        )
                    })
        }
        _ => true,
    }
}

/// Compare the already-authored plain layout to an actual encoding by borrow.
/// This performs no type import, layout allocation, value decoding or widening.
pub(crate) fn execution_carrier_matches_layout_observed<E: From<ChunkConstructionFailure>>(
    expected: &DataType,
    actual: &DataType,
    work: &mut impl FnMut(usize) -> Result<(), E>,
) -> Result<bool, E> {
    enum MatchError<E> {
        Caller(E),
        Source(novarocks_type_contract::ValueTypeError),
    }
    impl<E> From<novarocks_type_contract::ValueTypeError> for MatchError<E> {
        fn from(error: novarocks_type_contract::ValueTypeError) -> Self {
            Self::Source(error)
        }
    }
    novarocks_type_contract::data_type_layout_matches_carrier_observed(expected, actual, |units| {
        work(units).map_err(MatchError::Caller)
    })
    .map_err(|error| match error {
        MatchError::Caller(error) => error,
        MatchError::Source(error) => E::from(ChunkConstructionFailure::from(error)),
    })
}

/// The current engine's encoding set is independent of the pure vocabulary.
pub(crate) fn validate_chunk_carrier_encoding_observed<E: From<ChunkConstructionFailure>>(
    carrier: &DataType,
    observe: &mut impl FnMut(&DataType) -> Result<(), E>,
) -> Result<(), E> {
    novarocks_value_arrays::carrier::validate_engine_carrier_observed(carrier, observe).map_err(
        |failure| match failure {
            novarocks_value_arrays::ValueFailure::Caller(error) => error,
            novarocks_value_arrays::ValueFailure::Value(error) => {
                E::from(ChunkConstructionFailure::source(error))
            }
        },
    )
}

fn check_chunk_data_type(
    expected: &DataType,
    actual: &DataType,
    root_label: &str,
) -> Result<(), ChunkConstructionFailure> {
    check_exact(expected, actual).map_err(|source| schema_type_mismatch(root_label, source))
}

fn reconcile_chunk_field_to_data_type(
    expected: &Field,
    actual: &DataType,
    actual_nullable: bool,
) -> Result<Arc<Field>, ChunkConstructionFailure> {
    let data_type = reconcile_chunk_data_type(expected.data_type(), actual)?;
    let nullable = expected.is_nullable() || actual_nullable;
    if &data_type == expected.data_type() && nullable == expected.is_nullable() {
        Ok(Arc::new(expected.clone()))
    } else {
        Ok(Arc::new(rebuild_chunk_field(expected, data_type, nullable)))
    }
}

fn rebuild_chunk_field(expected: &Field, data_type: DataType, nullable: bool) -> Field {
    novarocks_type_contract::owned_resources::metadata_materialization::rebuild_original_field(
        expected, data_type, nullable,
    )
}

fn reconcile_chunk_data_type(
    expected: &DataType,
    actual: &DataType,
) -> Result<DataType, ChunkConstructionFailure> {
    reconcile_chunk_data_type_core(expected, actual, &mut ReconcileMetadataSources::None)
}

fn reconcile_chunk_data_type_core(
    expected: &DataType,
    actual: &DataType,
    sources: &mut ReconcileMetadataSources<'_>,
) -> Result<DataType, ChunkConstructionFailure> {
    if expected == actual {
        return Ok(expected.clone());
    }
    check_chunk_data_type(expected, actual, "column")?;
    if is_dictionary_string_carrier(expected, actual) {
        return Ok(actual.clone());
    }
    match (expected, actual) {
        (DataType::List(expected_field), DataType::List(actual_field)) => Ok(DataType::List(
            reconcile_nested_chunk_field(expected_field, actual_field, sources)?,
        )),
        (DataType::LargeList(expected_field), DataType::LargeList(actual_field)) => {
            Ok(DataType::LargeList(reconcile_nested_chunk_field(
                expected_field,
                actual_field,
                sources,
            )?))
        }
        (DataType::Map(expected_field, expected_ordered), DataType::Map(actual_field, _)) => {
            Ok(DataType::Map(
                reconcile_nested_chunk_field(expected_field, actual_field, sources)?,
                *expected_ordered,
            ))
        }
        (DataType::Struct(expected_fields), DataType::Struct(actual_fields)) => {
            let fields = expected_fields
                .iter()
                .zip(actual_fields.iter())
                .map(|(expected_field, actual_field)| {
                    reconcile_nested_chunk_field(expected_field, actual_field, sources)
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(DataType::Struct(fields.into()))
        }
        _ => Ok(expected.clone()),
    }
}

fn reconcile_nested_chunk_field(
    expected: &Arc<Field>,
    actual: &Arc<Field>,
    sources: &mut ReconcileMetadataSources<'_>,
) -> Result<Arc<Field>, ChunkConstructionFailure> {
    let data_type =
        reconcile_chunk_data_type_core(expected.data_type(), actual.data_type(), sources)?;
    let nullable = expected.is_nullable() || actual.is_nullable();
    match sources {
        ReconcileMetadataSources::None => {
            Ok(Arc::new(rebuild_chunk_field(expected, data_type, nullable)))
        }
        ReconcileMetadataSources::Original { original, emitted } => {
            let (field, loan) = original
                .rebuild_field_original(expected, data_type, nullable)
                .into_shared();
            if let Some(loan) = loan {
                emitted.push(loan);
            }
            Ok(field)
        }
    }
}

enum ReconcileMetadataSources<'a> {
    None,
    Original {
        original: &'a SchemaMetadataMaterializations,
        emitted: Vec<MetadataFieldLoan>,
    },
}
impl<'a> ReconcileMetadataSources<'a> {
    fn from_original(source: Option<&'a SchemaMetadataMaterializations>) -> Self {
        match source {
            None => Self::None,
            Some(original) => Self::Original {
                original,
                emitted: Vec::new(),
            },
        }
    }
    fn finish(self) -> Option<SchemaMetadataMaterializations> {
        match self {
            Self::None => None,
            Self::Original { original, emitted } => {
                Some(original.with_additional_original_loans(emitted))
            }
        }
    }
}

fn reconcile_owned_chunk_field(
    expected: &OriginalSharedField,
    actual: &DataType,
    actual_nullable: bool,
    sources: &mut ReconcileMetadataSources<'_>,
) -> Result<OriginalFieldMaterialization, ChunkConstructionFailure> {
    let data_type = reconcile_chunk_data_type_core(expected.field().data_type(), actual, sources)?;
    let nullable = expected.field().is_nullable() || actual_nullable;
    let field = if &data_type == expected.field().data_type()
        && nullable == expected.field().is_nullable()
    {
        expected.clone_original()
    } else {
        expected.rebuild_original(data_type, nullable)
    };
    // Preserve both original operations: root Arc publication, then the
    // original owned slot Field clone. The new loan follows that exact Arc.
    let (shared, loan) = field.into_shared();
    let result = match &loan {
        Some(loan) => OriginalFieldMaterialization::Materialized(
            loan.clone_original(&shared).expect("exact reconciled root"),
        ),
        None => OriginalFieldMaterialization::Plain(shared.as_ref().clone()),
    };
    Ok(result)
}

fn reconcile_owned_field(
    expected: &FieldRef,
    actual: &DataType,
    actual_nullable: bool,
    origins: &FieldMetadataOrigins,
    derived: &mut Vec<novarocks_types::arrow_metadata_owner::MetadataOwnedField>,
) -> Result<FieldRef, ChunkConstructionFailure> {
    let data_type = reconcile_owned_type(expected.data_type(), actual, origins, derived)?;
    let nullable = expected.is_nullable() || actual_nullable;
    let owner = origins.owner_for(expected).ok_or_else(|| {
        schema_contract(
            SchemaContractSite::Reconciliation,
            "nested chunk field metadata origin is missing",
        )
    })?;
    let owner = if &data_type == expected.data_type() && nullable == expected.is_nullable() {
        owner.clone()
    } else {
        owner
            .derive_field(
                data_type,
                nullable,
                MetadataOwnerLimits {
                    entries: 65536,
                    construction_bytes: 96 * 1024 * 1024,
                },
            )
            .map_err(ChunkConstructionFailure::from)?
    };
    if derived.len() >= 65536 {
        return Err(ChunkConstructionFailure::from(
            novarocks_types::arrow_metadata_owner::MetadataOwnerError::CapacityExceeded,
        ));
    }
    let field = Arc::clone(owner.field());
    derived.push(owner);
    Ok(field)
}

fn reconcile_owned_type(
    expected: &DataType,
    actual: &DataType,
    origins: &FieldMetadataOrigins,
    derived: &mut Vec<novarocks_types::arrow_metadata_owner::MetadataOwnedField>,
) -> Result<DataType, ChunkConstructionFailure> {
    // Validate the complete expected tree once per subtree before copying any
    // metadata. The index walk is bounded and does not inspect unknown tables.
    check_chunk_data_type(expected, actual, "column")?;
    if is_dictionary_string_carrier(expected, actual) {
        return Ok(actual.clone());
    }
    match (expected, actual) {
        (DataType::List(a), DataType::List(b)) => Ok(DataType::List(reconcile_owned_field(
            a,
            b.data_type(),
            b.is_nullable(),
            origins,
            derived,
        )?)),
        (DataType::LargeList(a), DataType::LargeList(b)) => Ok(DataType::LargeList(
            reconcile_owned_field(a, b.data_type(), b.is_nullable(), origins, derived)?,
        )),
        (DataType::Map(a, ordered), DataType::Map(b, _)) => Ok(DataType::Map(
            reconcile_owned_field(a, b.data_type(), b.is_nullable(), origins, derived)?,
            *ordered,
        )),
        (DataType::Struct(a), DataType::Struct(b)) => {
            let fields = a
                .iter()
                .zip(b)
                .map(|(a, b)| {
                    reconcile_owned_field(a, b.data_type(), b.is_nullable(), origins, derived)
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(DataType::Struct(fields.into()))
        }
        _ => Ok(expected.clone()),
    }
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
    /// Assemble typed occurrences without retaining external format metadata.
    /// The creating owner covers the Vec/map/schema wrapper construction first.
    pub fn try_new_typed(
        slots: Vec<ChunkSlotSchema>,
        storage: ValueTypeStorageLifetime,
    ) -> Result<Self, ChunkConstructionFailure> {
        Self::try_new_typed_observed(slots, storage, |_| Ok(()), |_| Ok(()))
    }

    /// Construct the actual schema scaffolds under the supplied owner's request
    /// port. The caller has already covered the input slot Vec before creating
    /// it. Every request describes this constructor's concrete allocation, not
    /// retained bytes or capacity inferred from a foreign collection.
    pub fn try_new_typed_observed<E: From<ChunkConstructionFailure>>(
        slots: Vec<ChunkSlotSchema>,
        storage: ValueTypeStorageLifetime,
        mut request: impl FnMut(std::alloc::Layout) -> Result<(), E>,
        mut work: impl FnMut(usize) -> Result<(), E>,
    ) -> Result<Self, E> {
        work(0)?;
        for slot in &slots {
            work(1)?;
            if !slot.is_typed() {
                return Err(E::from(ChunkConstructionFailure::contract(
                    ChunkSchemaViolation::LegacySlot,
                )));
            }
        }
        let count = slots.len();
        let table = novarocks_type_contract::owned_resources::hashmap::fresh_table_layout::<
            SlotId,
            usize,
        >(count)
        .map_err(|error| E::from(ChunkConstructionFailure::from(error)))?;
        if let Some(layout) = table.layout {
            request(layout)?;
        }
        let mut index_by_slot = HashMap::with_capacity(slots.len());
        request_array::<SlotId, E>(count, &mut request)?;
        let mut slot_ids = Vec::with_capacity(slots.len());
        request_array::<FieldRef, E>(count, &mut request)?;
        let mut fields = Vec::with_capacity(slots.len());
        for (index, slot) in slots.iter().enumerate() {
            work(3)?;
            if index_by_slot.insert(slot.slot_id(), index).is_some() {
                return Err(E::from(ChunkConstructionFailure::contract(
                    ChunkSchemaViolation::DuplicateSlot {
                        slot: slot.slot_id(),
                    },
                )));
            }
            slot_ids.push(slot.slot_id());
            fields.push(Arc::clone(slot.field_ref()));
        }
        request_arc_payload(
            std::alloc::Layout::array::<FieldRef>(count).map_err(|source| {
                E::from(ChunkConstructionFailure::array_request::<FieldRef>(
                    count, source,
                ))
            })?,
            &mut request,
        )?;
        let fields: arrow::datatypes::Fields = fields.into();
        request_arc_payload(std::alloc::Layout::new::<Schema>(), &mut request)?;
        Ok(Self {
            slots,
            arrow_schema: Arc::new(Schema::new(fields)),
            slot_ids,
            index_by_slot,
            metadata_materializations: None,
            field_metadata_origins: None,
            schema_metadata_origin: None,
            typed_representation: true,
            storage,
        })
    }

    /// Import explicit compiled bindings through the same occurrence author
    /// used by derived runtime declarations. No legacy importer is attempted.
    pub fn from_compiled_layout_owned_observed<E: From<ChunkConstructionFailure>>(
        layout: &novarocks_local_program::StaticLayout,
        storage: ValueTypeStorageLifetime,
        request: impl FnMut(std::alloc::Layout) -> Result<(), E>,
        work: impl FnMut(usize) -> Result<(), E>,
    ) -> Result<ChunkSchemaRef, E> {
        Self::from_compiled_layout_with_bindings_owned_observed(
            layout,
            storage,
            |index| layout.value_binding_at(index),
            request,
            work,
        )
    }

    /// The actual runtime producer supplies every complete value declaration.
    /// Names, slot IDs and unique IDs keep their original occurrence role;
    /// replacement types never come from format annotations or Arrow inference.
    /// Each newly authored Field and schema scaffold is admitted before creation.
    pub(crate) fn from_compiled_layout_with_bindings_owned_observed<
        'a,
        E: From<ChunkConstructionFailure>,
    >(
        layout: &'a novarocks_local_program::StaticLayout,
        storage: ValueTypeStorageLifetime,
        mut binding_at: impl FnMut(usize) -> Option<&'a TypedValueBinding>,
        mut request: impl FnMut(std::alloc::Layout) -> Result<(), E>,
        mut work: impl FnMut(usize) -> Result<(), E>,
    ) -> Result<ChunkSchemaRef, E> {
        work(0)?;
        if !layout.is_typed()
            || layout.schema().fields().len() != layout.slots().len()
            || layout.schema().metadata().as_arc().is_some()
        {
            return Err(E::from(ChunkConstructionFailure::contract(
                ChunkSchemaViolation::CanonicalLayout,
            )));
        }
        for (index, field) in layout.schema().fields().iter().enumerate() {
            work(2)?;
            if layout.value_binding_at(index).is_none()
                || binding_at(index).is_none()
                || layout.slot_metadata_at(index).is_none()
                || !execution_field_is_normalized_observed(field, &mut |units| {
                    work(units).map_err(ChunkObservationFailure::Caller)
                })
                .map_err(ChunkObservationFailure::into_caller)?
            {
                return Err(E::from(ChunkConstructionFailure::contract(
                    ChunkSchemaViolation::IncompleteOccurrence { index },
                )));
            }
        }
        let storage = retain_storage_observed(storage, &layout.storage_lifetime(), &mut request)?;
        request_array::<ChunkSlotSchema, E>(layout.slots().len(), &mut request)?;
        let mut slots = Vec::with_capacity(layout.slots().len());
        for (index, (field, slot)) in layout
            .schema()
            .fields()
            .iter()
            .zip(layout.slots())
            .enumerate()
        {
            work(3)?;
            let original = layout.value_binding_at(index).expect("validated binding");
            let binding = binding_at(index).ok_or_else(|| {
                E::from(ChunkConstructionFailure::contract(
                    ChunkSchemaViolation::MissingDerivedBinding { index },
                ))
            })?;
            let field = if std::ptr::eq(original, binding) {
                Arc::clone(field)
            } else {
                // This producer authors a new execution Field. Its name has
                // fresh backing; normalized type children share the complete
                // binding's immutable backing, whose owner travels with it.
                request_array::<u8, E>(field.name().len(), &mut request)?;
                request_arc_payload(std::alloc::Layout::new::<Field>(), &mut request)?;
                work(2)?;
                Arc::new(Field::new(
                    field.name(),
                    binding.data_type().clone(),
                    binding.nullable(),
                ))
            };
            let (_, unique_id) = layout
                .slot_metadata_at(index)
                .expect("validated slot declaration");
            slots.push(ChunkSlotSchema::try_new_typed_observed(
                *slot,
                field,
                binding
                    .clone_observed(|| work(1).map_err(ChunkObservationFailure::Caller))
                    .map_err(ChunkObservationFailure::into_caller)?,
                unique_id,
                storage.clone(),
                |step| match step {
                    ArrayNormalizationStep::InspectConstructionWork { units } => work(units),
                    _ => work(1),
                },
            )?);
        }
        let schema = Self::try_new_typed_observed(slots, storage, &mut request, &mut work)?;
        request_arc_payload(std::alloc::Layout::new::<ChunkSchema>(), &mut request)?;
        Ok(Arc::new(schema))
    }

    pub fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
        self.storage.clone()
    }

    pub fn is_typed(&self) -> bool {
        self.typed_representation
    }

    #[cfg(test)]
    pub(crate) fn from_static_layout(
        layout: &novarocks_local_program::StaticLayout,
    ) -> Result<ChunkSchemaRef, ChunkConstructionFailure> {
        if layout.is_typed() {
            return Self::from_typed_layout(layout);
        }
        if !layout.has_exact_slot_metadata() {
            return Err(schema_contract(
                SchemaContractSite::Layout,
                "local program layout has no exact slot metadata",
            ));
        }
        if layout.field_metadata_origins().is_some() {
            return Self::from_layout_with_shared_origins(layout, true);
        }
        let slots = layout
            .schema()
            .fields()
            .iter()
            .zip(layout.slots())
            .enumerate()
            .map(|(index, (field, slot))| {
                let (field_schema, unique_id) =
                    layout.slot_metadata_at(index).ok_or_else(|| {
                        schema_contract(
                            SchemaContractSite::Layout,
                            format_args!("local program slot {index} lacks metadata"),
                        )
                    })?;
                ChunkSlotSchema::try_new_owned_field(
                    *slot,
                    OriginalFieldMaterialization::clone_from_source(
                        field,
                        layout.metadata_materializations(),
                    ),
                    Some(ChunkFieldSchema::from_value_type(field_schema.clone())),
                    unique_id,
                )
            })
            .collect::<Result<Vec<_>, ChunkConstructionFailure>>()?;
        let metadata = match layout.metadata_materializations() {
            Some(source) => {
                ChunkMetadataSource::Original(source.schema_owner().clone_metadata_original())
            }
            None => ChunkMetadataSource::Foreign(layout.schema().metadata().clone()),
        };
        Ok(Arc::new(Self::try_new_with_metadata_source(
            slots,
            metadata,
            layout
                .metadata_materializations()
                .map(|source| source.fields()),
        )?))
    }

    /// Typed compilation supplies the complete immutable binding. The old
    /// Arrow annotation import remains only in the explicit expansion adapter.
    pub fn from_compiled_layout(
        layout: &novarocks_local_program::StaticLayout,
    ) -> Result<ChunkSchemaRef, ChunkConstructionFailure> {
        if layout.is_typed() {
            return Self::from_typed_layout(layout);
        }
        if layout.field_metadata_origins().is_some() {
            return Self::from_layout_with_shared_origins(layout, false);
        }
        let slots = layout
            .schema()
            .fields()
            .iter()
            .zip(layout.slots())
            .map(|(field, slot)| {
                ChunkSlotSchema::try_new_owned_field(
                    *slot,
                    OriginalFieldMaterialization::clone_from_source(
                        field,
                        layout.metadata_materializations(),
                    ),
                    None,
                    None,
                )
            })
            .collect::<Result<Vec<_>, ChunkConstructionFailure>>()?;
        if slots.len() != layout.schema().fields().len() {
            return Err(schema_contract(
                SchemaContractSite::Layout,
                "compiled layout slot and field counts differ",
            ));
        }
        let metadata = match layout.metadata_materializations() {
            Some(source) => {
                ChunkMetadataSource::Original(source.schema_owner().clone_metadata_original())
            }
            None => ChunkMetadataSource::Foreign(layout.schema().metadata().clone()),
        };
        Ok(Arc::new(Self::try_new_with_metadata_source(
            slots,
            metadata,
            layout
                .metadata_materializations()
                .map(|source| source.fields()),
        )?))
    }

    fn from_typed_layout(
        layout: &novarocks_local_program::StaticLayout,
    ) -> Result<ChunkSchemaRef, ChunkConstructionFailure> {
        if layout.schema().fields().len() != layout.slots().len()
            || layout.schema().metadata().as_arc().is_some()
        {
            return Err(schema_contract(
                SchemaContractSite::Layout,
                "typed layout does not have a canonical execution schema",
            ));
        }
        let storage = layout.storage_lifetime();
        let mut slots = Vec::with_capacity(layout.slots().len());
        for (index, (field, slot_id)) in layout
            .schema()
            .fields()
            .iter()
            .zip(layout.slots())
            .enumerate()
        {
            let binding = layout.value_binding_at(index).ok_or_else(|| {
                schema_contract(
                    SchemaContractSite::Layout,
                    format_args!("typed layout occurrence {index} has no explicit value binding"),
                )
            })?;
            if !execution_field_is_normalized(field)? {
                return Err(schema_contract(
                    SchemaContractSite::Layout,
                    format_args!("typed layout occurrence {index} has a noncanonical actual field"),
                ));
            }
            let (_, unique_id) = layout.slot_metadata_at(index).ok_or_else(|| {
                schema_contract(
                    SchemaContractSite::Layout,
                    format_args!("typed layout occurrence {index} has no exact slot declaration"),
                )
            })?;
            slots.push(ChunkSlotSchema::try_new_typed(
                *slot_id,
                field.clone(),
                binding.clone(),
                unique_id,
                storage.clone(),
            )?);
        }
        Self::try_new_typed(slots, storage).map(Arc::new)
    }

    fn from_layout_with_shared_origins(
        layout: &novarocks_local_program::StaticLayout,
        legacy: bool,
    ) -> Result<ChunkSchemaRef, ChunkConstructionFailure> {
        let origins = layout
            .field_metadata_origins()
            .expect("actual source route");
        let slots = layout
            .schema()
            .fields()
            .iter()
            .zip(layout.slots())
            .enumerate()
            .map(|(index, (field, slot))| {
                let (metadata, unique_id) = if legacy {
                    let (metadata, unique_id) =
                        layout.slot_metadata_at(index).ok_or_else(|| {
                            schema_contract(
                                SchemaContractSite::Mutation,
                                format_args!("local program slot {index} lacks metadata"),
                            )
                        })?;
                    (
                        Some(ChunkFieldSchema::from_value_type(metadata.clone())),
                        unique_id,
                    )
                } else {
                    (None, None)
                };
                let mut result = ChunkSlotSchema::try_new_with_metadata_origins(
                    *slot,
                    Arc::clone(field),
                    origins
                        .for_field_tree(field, 65536, 64)
                        .map_err(ChunkConstructionFailure::from)?,
                    metadata,
                    unique_id,
                )?;
                result.field = OriginalSharedField::from_shared(
                    Arc::clone(field),
                    layout.metadata_materializations(),
                );
                Ok(result)
            })
            .collect::<Result<Vec<_>, ChunkConstructionFailure>>()?;
        if slots.len() != layout.schema().fields().len() {
            return Err(schema_contract(
                SchemaContractSite::Mutation,
                "compiled layout slot and field counts differ",
            ));
        }
        let metadata = match layout.metadata_materializations() {
            Some(source) => {
                ChunkMetadataSource::Original(source.schema_owner().clone_metadata_original())
            }
            None => ChunkMetadataSource::Foreign(layout.schema().metadata().clone()),
        };
        let mut schema = Self::try_new_with_metadata_source(
            slots,
            metadata,
            layout
                .metadata_materializations()
                .map(|source| source.fields()),
        )?;
        if let Some(origin) = layout.schema_metadata_origin() {
            if origin.backing_bytes_for(layout.schema()).is_none() {
                return Err(schema_contract(
                    SchemaContractSite::Mutation,
                    "local program schema metadata owner conflicts with its layout",
                ));
            }
            schema.arrow_schema = Arc::clone(layout.schema());
            schema.schema_metadata_origin = Some(origin.clone());
            schema.metadata_materializations = layout.metadata_materializations().cloned();
        }
        Ok(Arc::new(schema))
    }

    pub fn try_new(slots: Vec<ChunkSlotSchema>) -> Result<Self, ChunkConstructionFailure> {
        let metadata = ArrowMetadataOwner::try_new(
            Vec::new(),
            MetadataOwnerLimits {
                entries: 0,
                construction_bytes: 0,
            },
        )
        .map_err(ChunkConstructionFailure::from)?;
        Self::try_new_with_owned_schema_metadata(slots, metadata)
    }

    pub fn try_new_with_schema_metadata(
        slots: Vec<ChunkSlotSchema>,
        metadata: impl Into<arrow::datatypes::Metadata>,
    ) -> Result<Self, ChunkConstructionFailure> {
        Self::try_new_with_metadata_source(
            slots,
            ChunkMetadataSource::Foreign(metadata.into()),
            None,
        )
    }

    /// Derive from this positively paired original Schema map and inherit its
    /// exact field loans. Structural equality never elects an original source.
    #[cfg(test)]
    pub(crate) fn try_new_with_original_schema_metadata(
        slots: Vec<ChunkSlotSchema>,
        source: &SchemaMetadataMaterializations,
    ) -> Result<Self, ChunkConstructionFailure> {
        Self::try_new_with_metadata_source(
            slots,
            ChunkMetadataSource::Original(source.schema_owner().clone_metadata_original()),
            Some(source.fields()),
        )
    }

    pub fn try_new_with_owned_schema_metadata(
        slots: Vec<ChunkSlotSchema>,
        metadata: ArrowMetadataOwner,
    ) -> Result<Self, ChunkConstructionFailure> {
        Self::try_new_with_metadata_source(slots, ChunkMetadataSource::Owned(metadata), None)
    }
    pub fn try_new_with_derived_schema_metadata(
        slots: Vec<ChunkSlotSchema>,
        source: &MetadataOwnedSchema,
    ) -> Result<Self, ChunkConstructionFailure> {
        Self::try_new_with_metadata_source(
            slots,
            ChunkMetadataSource::Derived(source.clone()),
            None,
        )
    }
    fn try_new_with_metadata_source(
        slots: Vec<ChunkSlotSchema>,
        metadata: ChunkMetadataSource,
        inherited: Option<&[MetadataFieldLoan]>,
    ) -> Result<Self, ChunkConstructionFailure> {
        let mut index_by_slot = HashMap::with_capacity(slots.len());
        let mut slot_ids = Vec::with_capacity(slots.len());
        let mut fields = Vec::with_capacity(slots.len());
        let mut loans = inherited.map(|source| {
            let mut loans = Vec::with_capacity(source.len() + slots.len());
            loans.extend(source.iter().cloned());
            loans
        });
        for (idx, slot) in slots.iter().enumerate() {
            if index_by_slot.insert(slot.slot_id(), idx).is_some() {
                return Err(schema_contract(
                    SchemaContractSite::Metadata,
                    format_args!(
                        "duplicate slot id {} in chunk schema contract at index {}",
                        slot.slot_id(),
                        idx
                    ),
                ));
            }
            slot_ids.push(slot.slot_id());
            let (field, loan) = if slot.field.shares_original_reference() {
                (Arc::clone(slot.field_ref()), slot.field.loan().cloned())
            } else {
                slot.field.clone_original().into_shared()
            };
            fields.push(field);
            if let (Some(loans), Some(loan)) = (loans.as_mut(), loan) {
                loans.push(loan);
            }
        }
        let field_metadata_origins = if slots.iter().all(|slot| slot.metadata_origins().is_some()) {
            let count = slots
                .iter()
                .try_fold(0usize, |count, slot| {
                    count
                        .checked_add(slot.metadata_origins().unwrap().owners().len())
                        .filter(|count| *count <= 65536)
                })
                .ok_or_else(|| {
                    schema_contract(
                        SchemaContractSite::Metadata,
                        "chunk schema metadata origin collection exceeds its node profile",
                    )
                })?;
            let mut owners = Vec::with_capacity(count);
            for slot in &slots {
                owners.extend_from_slice(slot.metadata_origins().unwrap().owners());
            }
            Some(
                FieldMetadataOrigins::try_new(owners, 65536)
                    .map_err(ChunkConstructionFailure::from)?,
            )
        } else {
            None
        };
        let (arrow_schema, metadata_materializations, schema_metadata_origin) = match metadata {
            ChunkMetadataSource::Foreign(metadata) => (
                Arc::new(arrow::datatypes::Schema::new_with_metadata(
                    fields, metadata,
                )),
                None,
                None,
            ),
            ChunkMetadataSource::EmptyOriginal(metadata) => {
                let schema = metadata.into_schema(fields).into_shared();
                let arrow_schema = schema.schema().clone();
                let origins = SchemaMetadataMaterializations::from_materialized_owners(
                    schema,
                    loans.unwrap_or_default().into(),
                );
                (arrow_schema, Some(origins), None)
            }
            ChunkMetadataSource::Original(metadata) => {
                let schema = metadata.into_schema(fields).into_shared();
                let arrow_schema = schema.schema().clone();
                let origins = SchemaMetadataMaterializations::from_materialized_owners(
                    schema,
                    loans.unwrap_or_default().into(),
                );
                (arrow_schema, Some(origins), None)
            }
            ChunkMetadataSource::Owned(metadata) => {
                let origin = metadata.into_schema(fields.into());
                (Arc::clone(origin.schema()), None, Some(origin))
            }
            ChunkMetadataSource::Derived(source) => {
                let origin = source
                    .derive_schema(
                        fields.into(),
                        MetadataOwnerLimits {
                            entries: 65536,
                            construction_bytes: 96 * 1024 * 1024,
                        },
                    )
                    .map_err(ChunkConstructionFailure::from)?;
                (Arc::clone(origin.schema()), None, Some(origin))
            }
        };
        Ok(Self {
            slots,
            arrow_schema,
            slot_ids,
            index_by_slot,
            metadata_materializations,
            field_metadata_origins,
            schema_metadata_origin,
            typed_representation: false,
            storage: ValueTypeStorageLifetime::pure(),
        })
    }

    pub fn empty() -> Self {
        Self::try_new(Vec::new()).expect("empty chunk schema construction is infallible")
    }

    pub fn field_metadata_origins(&self) -> Option<&FieldMetadataOrigins> {
        self.field_metadata_origins.as_ref()
    }

    pub fn schema_metadata_origin(&self) -> Option<&MetadataOwnedSchema> {
        self.schema_metadata_origin.as_ref()
    }

    /// Inspect only this immutable ChunkSchema's own scaffolds. Field/schema
    /// owners and their genuine metadata receipts are inspected separately.
    pub(super) fn inspect_root_scaffolds(
        &self,
        inspection: &mut super::root_schema_backing::RootSchemaInspection,
    ) -> Result<(), super::root_schema_backing::RootSchemaBackingError> {
        use super::root_schema_backing::RootSchemaBackingError;
        use std::alloc::Layout;
        use std::mem::{align_of, size_of};

        inspection.charge_arc(self)?;
        for (capacity, width) in [
            (self.slots.capacity(), size_of::<ChunkSlotSchema>()),
            (self.slot_ids.capacity(), size_of::<SlotId>()),
        ] {
            inspection.charge(
                capacity
                    .checked_mul(width)
                    .ok_or(RootSchemaBackingError::CapacityExceeded)?,
            )?;
        }
        // Private build creates this map fresh with capacity for all slots,
        // inserts each key once, and exposes no mutation/removal API. Its
        // never-deleted table history is known; no arbitrary map is accepted.
        if self.index_by_slot.capacity() != 0 {
            let buckets = self
                .index_by_slot
                .capacity()
                .checked_add(1)
                .and_then(usize::checked_next_power_of_two)
                .ok_or(RootSchemaBackingError::CapacityExceeded)?;
            let payload = buckets
                .checked_mul(size_of::<(SlotId, usize)>())
                .ok_or(RootSchemaBackingError::CapacityExceeded)?;
            let alignment = align_of::<(SlotId, usize)>().max(16);
            let aligned = payload
                .checked_add(alignment - 1)
                .ok_or(RootSchemaBackingError::CapacityExceeded)?
                & !(alignment - 1);
            let bytes = aligned
                .checked_add(buckets)
                .and_then(|b| b.checked_add(16))
                .ok_or(RootSchemaBackingError::CapacityExceeded)?;
            Layout::from_size_align(bytes, alignment)
                .map_err(|_| RootSchemaBackingError::CapacityExceeded)?;
            inspection.charge(bytes)?;
        }
        // Immutable semantic nodes carry their own creation storage lifetime.
        // Per-chunk scaffolds retain handles; they do not recount that backing.
        Ok(())
    }

    /// Positive original metadata construction loans, never a MEM grant.
    pub(crate) fn metadata_materializations(&self) -> Option<&SchemaMetadataMaterializations> {
        self.metadata_materializations.as_ref()
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
    ) -> Result<ChunkSlotSchema, ChunkConstructionFailure> {
        ChunkSlotSchema::from_field(slot_id, field, None)
    }

    pub fn try_ref_from_schema_and_slot_ids(
        schema: &Schema,
        slot_ids: &[SlotId],
    ) -> Result<ChunkSchemaRef, ChunkConstructionFailure> {
        if schema.fields().len() != slot_ids.len() {
            return Err(schema_contract(
                SchemaContractSite::Assembly,
                format_args!(
                    "chunk schema slot id length mismatch: schema_fields={} slot_ids={}",
                    schema.fields().len(),
                    slot_ids.len()
                ),
            ));
        }
        let slots = schema
            .fields()
            .iter()
            .zip(slot_ids.iter().copied())
            .map(|(field, slot_id)| Self::slot_schema_from_arrow_field(slot_id, field.as_ref()))
            .collect::<Result<Vec<_>, _>>()?;
        Self::try_new_with_schema_metadata(slots, schema.metadata().clone()).map(Arc::new)
    }

    /// Retains an exact immutable schema constructed by an owned decoder.
    /// Structural equality never substitutes for the field/map owner receipts.
    pub fn try_ref_from_owned_schema_and_slot_ids(
        source: &MetadataOwnedSchema,
        origins: &FieldMetadataOrigins,
        slot_ids: &[SlotId],
    ) -> Result<ChunkSchemaRef, ChunkConstructionFailure> {
        if source.schema().fields().len() != slot_ids.len() {
            return Err(schema_contract(
                SchemaContractSite::Assembly,
                "owned chunk schema slot id length mismatch",
            ));
        }
        let slots = source
            .schema()
            .fields()
            .iter()
            .zip(slot_ids)
            .map(|(field, slot)| {
                ChunkSlotSchema::try_new_with_metadata_origins(
                    *slot,
                    Arc::clone(field),
                    origins
                        .for_field_tree(field, 65536, 64)
                        .map_err(ChunkConstructionFailure::from)?,
                    None,
                    None,
                )
            })
            .collect::<Result<Vec<_>, ChunkConstructionFailure>>()?;
        let mut schema = Self::try_new(slots)?;
        schema.arrow_schema = Arc::clone(source.schema());
        schema.schema_metadata_origin = Some(source.clone());
        Ok(Arc::new(schema))
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

    pub fn project_by_slots(&self, slot_ids: &[SlotId]) -> Result<Self, ChunkConstructionFailure> {
        if self.is_typed() {
            return Err(schema_contract(
                SchemaContractSite::Projection,
                "typed schema projection requires a supplied creation lifetime",
            ));
        }
        self.project_by_slots_core(slot_ids, None)
    }

    pub fn project_by_slots_typed(
        &self,
        slot_ids: &[SlotId],
        storage: ValueTypeStorageLifetime,
    ) -> Result<Self, ChunkConstructionFailure> {
        if !self.is_typed() {
            return Err(schema_contract(
                SchemaContractSite::Projection,
                "typed schema projection requires an explicit typed source",
            ));
        }
        self.project_by_slots_core(slot_ids, Some(storage))
    }

    fn project_by_slots_core(
        &self,
        slot_ids: &[SlotId],
        storage: Option<ValueTypeStorageLifetime>,
    ) -> Result<Self, ChunkConstructionFailure> {
        let mut slots = Vec::with_capacity(slot_ids.len());
        for slot_id in slot_ids {
            let slot = self.slot(*slot_id).cloned().ok_or_else(|| {
                schema_contract(
                    SchemaContractSite::Projection,
                    format_args!(
                        "chunk schema projection references missing slot {} (available={:?})",
                        slot_id,
                        self.slot_ids()
                    ),
                )
            })?;
            slots.push(slot);
        }
        if let Some(storage) = storage {
            return Self::try_new_typed(slots, storage);
        }
        match self.metadata_materializations() {
            Some(source) => Self::try_new_with_metadata_source(
                slots,
                ChunkMetadataSource::EmptyOriginal(
                    MaterializedMetadataMap::new_for_original_reserve(),
                ),
                Some(source.fields()),
            ),
            None => Self::try_new(slots),
        }
    }

    pub fn with_fields_in_order(
        &self,
        fields: Vec<Field>,
    ) -> Result<Self, ChunkConstructionFailure> {
        if fields.len() != self.slots.len() {
            return Err(schema_contract(
                SchemaContractSite::Projection,
                format_args!(
                    "chunk schema field length mismatch: fields={} slots={}",
                    fields.len(),
                    self.slots.len()
                ),
            ));
        }
        let slots = self
            .slots
            .iter()
            .cloned()
            .zip(fields)
            .map(|(slot, field)| slot.with_field(field))
            .collect::<Result<Vec<_>, _>>()?;
        Self::try_new(slots)
    }

    pub fn concat(parts: &[ChunkSchemaRef]) -> Result<Self, ChunkConstructionFailure> {
        if parts.iter().any(|part| part.is_typed()) {
            return Err(schema_contract(
                SchemaContractSite::Projection,
                "typed schema concatenation requires a supplied creation lifetime",
            ));
        }
        let mut slots = Vec::new();
        let mut origins = None::<Vec<MetadataFieldLoan>>;
        for part in parts {
            slots.extend_from_slice(part.slots());
            if let Some(source) = part.metadata_materializations() {
                origins
                    .get_or_insert_with(Vec::new)
                    .extend(source.fields().iter().cloned());
            }
        }
        match origins {
            Some(origins) => Self::try_new_with_metadata_source(
                slots,
                ChunkMetadataSource::EmptyOriginal(
                    MaterializedMetadataMap::new_for_original_reserve(),
                ),
                Some(&origins),
            ),
            None => Self::try_new(slots),
        }
    }

    pub fn concat_typed(
        parts: &[ChunkSchemaRef],
        storage: ValueTypeStorageLifetime,
    ) -> Result<Self, ChunkConstructionFailure> {
        if parts.iter().any(|part| !part.is_typed()) {
            return Err(schema_contract(
                SchemaContractSite::Projection,
                "typed schema concatenation requires explicit typed sources",
            ));
        }
        let count = parts.iter().try_fold(0usize, |count, part| {
            count
                .checked_add(part.slots().len())
                .ok_or_else(|| ChunkConstructionFailure::from(novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic))
        })?;
        let mut slots = Vec::with_capacity(count);
        for part in parts {
            slots.extend_from_slice(part.slots());
        }
        Self::try_new_typed(slots, storage)
    }
}

pub(super) fn align_chunk_schema_to_batch(
    batch: &RecordBatch,
    chunk_schema: &ChunkSchema,
) -> Result<ChunkSchemaRef, ChunkConstructionFailure> {
    if batch.num_columns() != chunk_schema.slots().len() {
        return Err(schema_contract(
            SchemaContractSite::Reconciliation,
            format_args!(
                "chunk schema contract length mismatch: batch_columns={} contract_slots={}",
                batch.num_columns(),
                chunk_schema.slots().len()
            ),
        ));
    }
    let mut sources =
        ReconcileMetadataSources::from_original(chunk_schema.metadata_materializations());
    let mut slots = Vec::with_capacity(batch.num_columns());
    for (idx, field) in batch.schema().fields().iter().enumerate() {
        let expected = chunk_schema.slots().get(idx).ok_or_else(|| {
            schema_contract(
                SchemaContractSite::Reconciliation,
                format_args!("missing chunk schema slot at index {}", idx),
            )
        })?;
        // Arrow field nullability is producer metadata here. A nullable batch
        // can flow through a non-nullable contract because source-level NOT
        // NULL enforcement happens downstream, and a non-nullable batch is a
        // valid runtime instance of a nullable contract.
        if field.name() != expected.name() {
            return Err(schema_contract(
                SchemaContractSite::Reconciliation,
                format_args!(
                    "chunk schema field mismatch at index {}: batch=({}, {:?}, {}) contract=({}, {:?}, {})",
                    idx,
                    field.name(),
                    field.data_type(),
                    field.is_nullable(),
                    expected.name(),
                    expected.data_type(),
                    expected.nullable()
                ),
            ));
        }
        let root = format!("slot {} ({})", expected.slot_id(), expected.name());
        check_chunk_data_type(expected.data_type(), field.data_type(), &root)?;
        if expected.metadata_origins().is_some() {
            slots.push(expected.reconcile_to_carrier(field.data_type(), field.is_nullable())?);
        } else if chunk_schema.metadata_materializations().is_some() {
            let field = reconcile_owned_chunk_field(
                &expected.field,
                field.data_type(),
                field.is_nullable(),
                &mut sources,
            )?;
            slots.push(ChunkSlotSchema::try_new_owned_field(
                expected.slot_id(),
                field,
                Some(expected.field_schema.clone()),
                expected.unique_id,
            )?);
        } else {
            let reconciled_field = reconcile_chunk_field_to_data_type(
                expected.field(),
                field.data_type(),
                field.is_nullable(),
            )?;
            slots.push(
                expected.with_field_and_slot_id(
                    expected.slot_id(),
                    reconciled_field.as_ref().clone(),
                )?,
            );
        }
    }
    let materializations = sources.finish();
    let schema = if let Some(source) = chunk_schema.schema_metadata_origin() {
        ChunkSchema::try_new_with_derived_schema_metadata(slots, source)?
    } else {
        match materializations.as_ref() {
            Some(source) => ChunkSchema::try_new_with_metadata_source(
                slots,
                ChunkMetadataSource::EmptyOriginal(
                    MaterializedMetadataMap::new_for_original_reserve(),
                ),
                Some(source.fields()),
            )?,
            None => ChunkSchema::try_new(slots)?,
        }
    };
    Ok(Arc::new(schema))
}

pub(super) fn align_chunk_schema_to_columns(
    columns: &[ArrayRef],
    chunk_schema: &ChunkSchema,
) -> Result<ChunkSchemaRef, ChunkConstructionFailure> {
    if columns.len() != chunk_schema.slots().len() {
        return Err(schema_contract(
            SchemaContractSite::Reconciliation,
            format_args!(
                "chunk schema contract length mismatch: columns={} contract_slots={}",
                columns.len(),
                chunk_schema.slots().len()
            ),
        ));
    }
    let mut sources =
        ReconcileMetadataSources::from_original(chunk_schema.metadata_materializations());
    let mut slots = Vec::with_capacity(columns.len());
    for (idx, column) in columns.iter().enumerate() {
        let expected = chunk_schema.slots().get(idx).ok_or_else(|| {
            schema_contract(
                SchemaContractSite::Reconciliation,
                format_args!("missing chunk schema slot at index {}", idx),
            )
        })?;
        let root = format!("slot {} ({})", expected.slot_id(), expected.name());
        check_chunk_data_type(expected.data_type(), column.data_type(), &root)?;
        if expected.metadata_origins().is_some() {
            slots.push(expected.reconcile_to_carrier(column.data_type(), column.null_count() > 0)?);
        } else if chunk_schema.metadata_materializations().is_some() {
            let field = reconcile_owned_chunk_field(
                &expected.field,
                column.data_type(),
                column.null_count() > 0,
                &mut sources,
            )?;
            slots.push(ChunkSlotSchema::try_new_owned_field(
                expected.slot_id(),
                field,
                Some(expected.field_schema.clone()),
                expected.unique_id,
            )?);
        } else {
            let reconciled_field = reconcile_chunk_field_to_data_type(
                expected.field(),
                column.data_type(),
                column.null_count() > 0,
            )?;
            slots.push(
                expected.with_field_and_slot_id(
                    expected.slot_id(),
                    reconciled_field.as_ref().clone(),
                )?,
            );
        }
    }
    let materializations = sources.finish();
    let schema = if let Some(source) = chunk_schema.schema_metadata_origin() {
        ChunkSchema::try_new_with_derived_schema_metadata(slots, source)?
    } else {
        match materializations.as_ref() {
            Some(source) => ChunkSchema::try_new_with_metadata_source(
                slots,
                ChunkMetadataSource::EmptyOriginal(
                    MaterializedMetadataMap::new_for_original_reserve(),
                ),
                Some(source.fields()),
            )?,
            None => ChunkSchema::try_new(slots)?,
        }
    };
    Ok(Arc::new(schema))
}

#[cfg(test)]
mod tests {
    use novarocks_type_contract::ValueLogicalType;
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::{
        Array, ArrayRef, BinaryArray, Decimal128Array, Int8Array, Int32Array, Int64Array, MapArray,
        StringArray, StructArray, TimestampMicrosecondArray,
    };
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;

    use super::{ChunkSchema, ChunkSlotSchema};
    use crate::exec::chunk::Chunk;
    use novarocks_types::SlotId;
    use novarocks_types::logical::{LogicalType, field_with_logical_type, logical_type_of_field};

    fn owned_slot(id: u32) -> ChunkSlotSchema {
        use novarocks_types::arrow_metadata_owner::{
            ArrowMetadataOwner, FieldMetadataOrigins, MetadataOwnerLimits,
        };
        let owner = ArrowMetadataOwner::try_new(
            vec![("nr_logical_type".to_string(), "json".to_string())],
            MetadataOwnerLimits {
                entries: 1,
                construction_bytes: 4096,
            },
        )
        .unwrap()
        .into_field(format!("v{id}"), DataType::Utf8, false);
        ChunkSlotSchema::try_new_with_metadata_origins(
            SlotId::new(id),
            Arc::clone(owner.field()),
            FieldMetadataOrigins::try_new(vec![owner], 1).unwrap(),
            None,
            Some(id as i32),
        )
        .unwrap()
    }

    #[test]
    fn owned_slot_derivation_preserves_identity_only_for_unchanged_field() {
        let source = owned_slot(7);
        let unchanged = source
            .with_type_and_nullable(DataType::Utf8, false)
            .unwrap();
        assert!(Arc::ptr_eq(source.field_ref(), unchanged.field_ref()));
        let renamed_slot = source.with_slot_id(SlotId::new(9)).unwrap();
        assert!(Arc::ptr_eq(source.field_ref(), renamed_slot.field_ref()));
        assert!(
            renamed_slot
                .metadata_origins()
                .unwrap()
                .metadata_bytes_for(source.field_ref())
                .is_some()
        );
        let nullable = source.with_nullable(true);
        assert!(!Arc::ptr_eq(source.field_ref(), nullable.field_ref()));
        assert_eq!(source.field().metadata(), nullable.field().metadata());
        assert!(
            nullable
                .metadata_origins()
                .unwrap()
                .metadata_bytes_for(nullable.field_ref())
                .is_some()
        );
        assert!(
            nullable
                .metadata_origins()
                .unwrap()
                .metadata_bytes_for(source.field_ref())
                .is_none()
        );
        let arbitrary = source.with_field(source.field().clone()).unwrap();
        assert_eq!(source, arbitrary);
        assert!(arbitrary.metadata_origins().is_none());
    }

    #[test]
    fn nested_carrier_alignment_derives_every_changed_source_field_owner() {
        use novarocks_types::arrow_metadata_owner::{
            ArrowMetadataOwner, FieldMetadataOrigins, MetadataOwnerLimits,
        };
        let limits = MetadataOwnerLimits {
            entries: 1,
            construction_bytes: 4096,
        };
        let item =
            ArrowMetadataOwner::try_new(vec![("nr_logical_type".into(), "json".into())], limits)
                .unwrap()
                .into_field("item".into(), DataType::Utf8, false);
        let list = ArrowMetadataOwner::try_new(vec![], limits)
            .unwrap()
            .into_field(
                "values".into(),
                DataType::List(Arc::clone(item.field())),
                false,
            );
        let root = ArrowMetadataOwner::try_new(vec![], limits)
            .unwrap()
            .into_field(
                "root".into(),
                DataType::Struct(vec![Arc::clone(list.field())].into()),
                false,
            );
        let old_item = Arc::clone(item.field());
        let old_list = Arc::clone(list.field());
        let source = ChunkSchema::try_new(vec![
            ChunkSlotSchema::try_new_with_metadata_origins(
                SlotId::new(1),
                Arc::clone(root.field()),
                FieldMetadataOrigins::try_new(vec![item, list, root], 3).unwrap(),
                None,
                None,
            )
            .unwrap(),
        ])
        .unwrap();
        let actual_item = Arc::new(Field::new(
            "item",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ));
        let actual_list = Arc::new(Field::new(
            "values",
            DataType::List(Arc::clone(&actual_item)),
            true,
        ));
        let actual_type = DataType::Struct(vec![actual_list].into());
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "root",
                actual_type.clone(),
                true,
            )])),
            vec![arrow::array::new_empty_array(&actual_type)],
        )
        .unwrap();
        let aligned = super::align_chunk_schema_to_batch(&batch, &source).unwrap();
        let slot = &aligned.slots()[0];
        let origins = slot.metadata_origins().unwrap();
        let reachable = origins.for_field_tree(slot.field_ref(), 5, 2).unwrap();
        assert_eq!(reachable.owners().len(), 3);
        let DataType::Struct(fields) = slot.data_type() else {
            panic!("struct")
        };
        let DataType::List(item) = fields[0].data_type() else {
            panic!("list")
        };
        assert!(!Arc::ptr_eq(&old_item, item));
        assert!(!Arc::ptr_eq(&old_list, &fields[0]));
        assert_eq!(logical_type_of_field(item), Some(LogicalType::Json));
        assert!(item.is_nullable());
        assert!(origins.metadata_bytes_for(item).is_some());
        assert!(origins.metadata_bytes_for(&fields[0]).is_some());
        assert!(origins.metadata_bytes_for(&actual_item).is_none());
        assert!(
            origins
                .metadata_bytes_for(&batch.schema().fields()[0])
                .is_none()
        );
        assert_eq!(
            super::align_chunk_schema_to_batch(&batch, &aligned).unwrap(),
            aligned
        );
    }

    #[test]
    fn retagged_actual_nested_array_shares_only_the_exact_known_target_field() {
        use novarocks_types::arrow_metadata_owner::{
            ArrowMetadataOwner, FieldMetadataOrigins, MetadataOwnerLimits,
        };
        let limits = MetadataOwnerLimits {
            entries: 1,
            construction_bytes: 4096,
        };
        let child =
            ArrowMetadataOwner::try_new(vec![("nr_logical_type".into(), "json".into())], limits)
                .unwrap()
                .into_field("item".into(), DataType::Utf8, true);
        let exact_child = Arc::clone(child.field());
        let root = ArrowMetadataOwner::try_new(vec![], limits)
            .unwrap()
            .into_field(
                "root".into(),
                DataType::Struct(vec![Arc::clone(&exact_child)].into()),
                false,
            );
        let schema = Arc::new(
            ChunkSchema::try_new(vec![
                ChunkSlotSchema::try_new_with_metadata_origins(
                    SlotId::new(1),
                    Arc::clone(root.field()),
                    FieldMetadataOrigins::try_new(vec![child, root], 2).unwrap(),
                    None,
                    None,
                )
                .unwrap(),
            ])
            .unwrap(),
        );
        let foreign = Arc::new(Field::new("item", DataType::Utf8, true));
        let values = Arc::new(StructArray::new(
            vec![Arc::clone(&foreign)].into(),
            vec![Arc::new(StringArray::from(vec![Some("{}")]))],
            None,
        )) as ArrayRef;
        let chunk = Chunk::try_new_with_columns(schema, vec![values]).unwrap();
        let DataType::Struct(actual_fields) = chunk.columns()[0].data_type() else {
            panic!("struct")
        };
        assert!(Arc::ptr_eq(&exact_child, &actual_fields[0]));
        assert!(!Arc::ptr_eq(&foreign, &actual_fields[0]));
        let origins = chunk.chunk_schema().field_metadata_origins().unwrap();
        assert!(origins.metadata_bytes_for(&actual_fields[0]).is_some());
        assert!(origins.metadata_bytes_for(&foreign).is_none());
        assert!(Arc::ptr_eq(
            &chunk.schema(),
            &chunk.chunk_schema().arrow_schema_ref()
        ));
    }

    #[test]
    fn static_and_compiled_layouts_keep_complete_raw_domains_and_legacy_projection() {
        use novarocks_type_contract::FunctionValueType;
        use novarocks_type_contract::{NR_LOGICAL_TYPE_KEY, ValueLogicalType};
        for (logical, carrier) in [
            (ValueLogicalType::Physical, DataType::FixedSizeBinary(16)),
            (ValueLogicalType::LargeInt, DataType::FixedSizeBinary(16)),
            (ValueLogicalType::Uuid, DataType::FixedSizeBinary(16)),
            (ValueLogicalType::Variant, DataType::LargeBinary),
            (ValueLogicalType::Json, DataType::Utf8),
            (ValueLogicalType::Hll, DataType::Binary),
            (ValueLogicalType::Bitmap, DataType::Binary),
            (ValueLogicalType::Object, DataType::Binary),
            (ValueLogicalType::Percentile, DataType::Binary),
        ] {
            let mut field = Field::new("value", carrier, true);
            if let Some(marker) = logical.metadata_value() {
                field = field.with_metadata(std::collections::HashMap::<String, String>::from([(
                    NR_LOGICAL_TYPE_KEY.into(),
                    marker.into(),
                )]));
            }
            let source = ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                SlotId::new(1),
                field.clone(),
                None,
                None,
            )])
            .unwrap();
            assert_eq!(source.slots()[0].field(), &field);
            let declaration =
                FunctionValueType::try_with_logical_type(field.data_type().clone(), true, logical)
                    .unwrap();
            let layout = crate::exec::chunk::typed_fixture::layout(
                Arc::new(Schema::new(vec![Field::new(
                    "value",
                    field.data_type().clone(),
                    true,
                )])),
                &[SlotId::new(1)],
                std::slice::from_ref(&declaration),
            );
            for restored in [
                ChunkSchema::from_static_layout(&layout).unwrap(),
                ChunkSchema::from_compiled_layout(&layout).unwrap(),
            ] {
                let slot = &restored.slots()[0];
                assert!(slot.field().metadata().as_arc().is_none());
                assert_eq!(slot.typed_binding(), declaration.typed_binding());
                assert_eq!(slot.field().data_type(), field.data_type());
                assert_eq!(slot.field().is_nullable(), field.is_nullable());
                assert_eq!(slot.field().name(), field.name());
                let legacy = declaration.try_to_legacy_field_projection("value").unwrap();
                assert_eq!(&legacy, &field);
                assert_eq!(
                    novarocks_type_contract::field_logical_type(&legacy).unwrap(),
                    logical
                );
            }
        }
    }

    #[test]
    fn static_layout_thaw_narrows_each_slot_and_keeps_actual_schema_owner() {
        use crate::exec::pipeline::operator::input_construction::{
            ConstructionFailure, try_runtime_input_construction_factory,
        };
        use novarocks_local_program::{LayoutCreationFailure, StaticLayout};
        use novarocks_type_contract::owned_resources::layout::arc_layout;
        use novarocks_type_contract::{
            CompileControlError, CompilePhase, FunctionValueType, PureCompileControl,
            TypedValueBinding,
        };
        struct Control;
        impl PureCompileControl for Control {
            fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
                assert!(units <= 256);
                Ok(())
            }
        }
        // Original legitimate source annotations/IDs/NULL and exact 257-field
        // receipt population remain independent of the execution declaration.
        let original_source = ChunkSchema::try_new((0..257).map(owned_slot).collect()).unwrap();
        assert_eq!(
            original_source
                .field_metadata_origins()
                .unwrap()
                .owners()
                .len(),
            257
        );
        for slot in original_source.slots() {
            let narrow = original_source
                .field_metadata_origins()
                .unwrap()
                .for_field_tree(slot.field_ref(), 65536, 64)
                .unwrap();
            assert_eq!(narrow.owners().len(), 1);
            assert!(narrow.metadata_bytes_for(slot.field_ref()).is_some());
            assert_eq!(slot.field().metadata()["nr_logical_type"], "json");
            assert!(!slot.nullable());
        }
        let declaration =
            FunctionValueType::try_with_logical_type(DataType::Utf8, false, ValueLogicalType::Json)
                .unwrap();
        let types = vec![declaration.clone(); 257];
        let slots = original_source.slot_ids();
        let schema = Arc::new(Schema::new(
            (0..257)
                .map(|id| Field::new(format!("v{id}"), DataType::Utf8, false))
                .collect::<Vec<_>>(),
        ));
        let declared = crate::exec::chunk::typed_fixture::layout(schema.clone(), slots, &types);
        let factory = try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            crate::runtime::mem_tracker::MemTracker::new_root("actual static thaw"),
            Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default()),
        )
        .unwrap();
        let owner = factory
            .begin_compiled_layout_construction(&declared)
            .unwrap();
        owner
            .request(arc_layout(std::alloc::Layout::array::<SlotId>(slots.len()).unwrap()).unwrap())
            .unwrap();
        let slot_ids = Arc::from(slots);
        owner
            .request(
                std::alloc::Layout::array::<(TypedValueBinding, Option<i32>)>(slots.len()).unwrap(),
            )
            .unwrap();
        let bindings = types
            .iter()
            .zip(original_source.slots())
            .map(|(ty, slot)| (ty.typed_binding().clone(), slot.unique_id()))
            .collect();
        let layout = StaticLayout::try_new_typed_with_creator(
            schema.clone(),
            slot_ids,
            bindings,
            owner.storage_lifetime(),
            |layout| {
                owner
                    .request(layout)
                    .map_err(LayoutCreationFailure::Creator)
            },
            &Control,
        )
        .unwrap();
        // The old nullable=true semantic claim contradicts its non-null field.
        // It remains an explicit strong negative rather than a weak success.
        let widening =
            FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
                .unwrap();
        let negative = StaticLayout::try_new_typed_with_creator(
            schema.clone(),
            Arc::from(slots),
            vec![(widening.typed_binding().clone(), None); 257],
            owner.storage_lifetime(),
            |layout| {
                owner
                    .request(layout)
                    .map_err(LayoutCreationFailure::Creator)
            },
            &Control,
        );
        assert!(matches!(
            negative,
            Err(LayoutCreationFailure::<ConstructionFailure>::Layout(
                novarocks_local_program::LayoutCompileError::Layout(
                    novarocks_local_program::LayoutError::ValueBindingMismatch
                )
            ))
        ));
        let typed_source = factory.create_compiled_chunk_schema(&layout).unwrap();
        let restored = factory.create_compiled_chunk_schema(&layout).unwrap();
        assert_eq!(*typed_source, *restored);
        assert!(restored.is_typed());
        assert_eq!(restored.slot_ids(), slots);
        assert!(restored.arrow_schema_ref().metadata().as_arc().is_none());
        for (index, new) in restored.slots().iter().enumerate() {
            assert!(Arc::ptr_eq(
                &layout.schema().fields()[index],
                new.field_ref()
            ));
            assert_eq!(new.typed_binding(), declaration.typed_binding());
            assert_eq!(new.unique_id(), Some(index as i32));
            assert!(new.field().metadata().as_arc().is_none());
            assert!(!new.nullable());
        }
        let projected_owner = factory.begin_compiled_layout_construction(&layout).unwrap();
        let projected = layout
            .project_by_slots_typed_with_creator(
                &[SlotId::new(19), SlotId::new(3)],
                projected_owner.storage_lifetime(),
                |request| {
                    projected_owner
                        .request(request)
                        .map_err(LayoutCreationFailure::Creator)
                },
                &Control,
            )
            .unwrap();
        let restored = factory.create_compiled_chunk_schema(&projected).unwrap();
        assert_eq!(restored.slot_ids(), &[SlotId::new(19), SlotId::new(3)]);
        assert!(Arc::ptr_eq(
            &layout.schema().fields()[19],
            restored.slots()[0].field_ref()
        ));
        assert_eq!(
            restored.slots()[0].typed_binding(),
            declaration.typed_binding()
        );
        // Preserve the source thaw/projection population and exact schema
        // receipts on the actual source owner, never on an equal execution copy.
        let source_restored = ChunkSchema::try_ref_from_owned_schema_and_slot_ids(
            original_source.schema_metadata_origin().unwrap(),
            original_source.field_metadata_origins().unwrap(),
            slots,
        )
        .unwrap();
        assert_eq!(
            original_source.arrow_schema_ref(),
            source_restored.arrow_schema_ref()
        );
        assert_eq!(original_source.slot_ids(), source_restored.slot_ids());
        // Source-schema loans contain no execution unique-ID channel. The
        // actual typed thaw above preserves every original Some(id) exactly.
        for (id, slot) in original_source.slots().iter().enumerate() {
            assert_eq!(slot.unique_id(), Some(id as i32));
        }
        assert!(Arc::ptr_eq(
            &original_source.arrow_schema_ref(),
            &source_restored.arrow_schema_ref()
        ));
        assert_eq!(
            source_restored
                .field_metadata_origins()
                .unwrap()
                .owners()
                .len(),
            257
        );
        for (old, new) in original_source.slots().iter().zip(source_restored.slots()) {
            assert!(Arc::ptr_eq(old.field_ref(), new.field_ref()));
            assert_eq!(new.metadata_origins().unwrap().owners().len(), 1);
        }
        let source_projection = original_source
            .schema_metadata_origin()
            .unwrap()
            .derive_schema(
                vec![
                    original_source.slots()[19].field_ref().clone(),
                    original_source.slots()[3].field_ref().clone(),
                ]
                .into(),
                novarocks_types::arrow_metadata_owner::MetadataOwnerLimits {
                    entries: 0,
                    construction_bytes: 0,
                },
            )
            .unwrap();
        let source_restored = ChunkSchema::try_ref_from_owned_schema_and_slot_ids(
            &source_projection,
            original_source.field_metadata_origins().unwrap(),
            &[SlotId::new(19), SlotId::new(3)],
        )
        .unwrap();
        assert_eq!(
            source_restored.slot_ids(),
            &[SlotId::new(19), SlotId::new(3)]
        );
        assert!(Arc::ptr_eq(
            original_source.slots()[19].field_ref(),
            source_restored.slots()[0].field_ref()
        ));
        assert_eq!(
            source_restored
                .field_metadata_origins()
                .unwrap()
                .owners()
                .len(),
            2
        );
        assert!(
            source_restored
                .schema_metadata_origin()
                .unwrap()
                .backing_bytes_for(&source_restored.arrow_schema_ref())
                .is_some()
        );
    }

    #[test]
    fn typed_layout_import_keeps_explicit_nested_domain_and_real_creation_hold() {
        use novarocks_local_program::StaticLayout;
        use novarocks_type_contract::{
            CompileControlError, CompilePhase, PureCompileControl, TypedValueBinding, ValueField,
            ValueKind, ValueLayout, ValueLogicalType, ValueTypeRef, ValueTypeStorageHold,
            ValueTypeStorageLifetime,
        };
        use std::sync::atomic::{AtomicUsize, Ordering};

        #[derive(Debug)]
        enum ProbeFailure {
            Construction,
            Request(usize),
            Work,
        }
        impl From<crate::exec::chunk::construction_failure::ChunkConstructionFailure> for ProbeFailure {
            fn from(
                _source: crate::exec::chunk::construction_failure::ChunkConstructionFailure,
            ) -> Self {
                Self::Construction
            }
        }
        struct Control;
        impl PureCompileControl for Control {
            fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
                Ok(())
            }
        }
        struct Hold(Arc<AtomicUsize>);
        impl ValueTypeStorageHold for Hold {}
        impl Drop for Hold {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let drops = Arc::new(AtomicUsize::new(0));
        let storage = ValueTypeStorageLifetime::held(Arc::new(Hold(drops.clone())));
        let child = ValueTypeRef::try_new(
            ValueKind::Nominal(ValueLogicalType::Json),
            true,
            storage.clone(),
        )
        .unwrap();
        let value = ValueTypeRef::try_new(
            ValueKind::Struct(
                vec![ValueField::new("payload", child, storage.clone())].into_boxed_slice(),
            ),
            false,
            storage.clone(),
        )
        .unwrap();
        let carrier = DataType::Struct(vec![Field::new("payload", DataType::Utf8, true)].into());
        let field = Arc::new(Field::new("output", carrier.clone(), false));
        let binding = TypedValueBinding::try_new(
            value,
            ValueLayout::try_new(&carrier, storage.clone()).unwrap(),
        )
        .unwrap();
        let layout = StaticLayout::try_new_typed_for_compile(
            Arc::new(Schema::new(vec![field.clone()])),
            Arc::from([SlotId::new(7)]),
            vec![(binding, Some(91))],
            storage.clone(),
            |_| Ok(()),
            &Control,
        )
        .unwrap();
        let restored = ChunkSchema::from_compiled_layout(&layout).unwrap();
        let static_restored = ChunkSchema::from_static_layout(&layout).unwrap();
        let mut requests = Vec::new();
        let funded_restored = ChunkSchema::from_compiled_layout_owned_observed::<ProbeFailure>(
            &layout,
            storage.clone(),
            |request| {
                requests.push(request);
                Ok(())
            },
            |_| Ok(()),
        )
        .unwrap();
        assert_eq!(requests.len(), 7);
        assert_eq!(
            requests[0],
            std::alloc::Layout::array::<ChunkSlotSchema>(1).unwrap()
        );
        for stop in 0..requests.len() {
            let mut seen = 0;
            let rejected = ChunkSchema::from_compiled_layout_owned_observed::<ProbeFailure>(
                &layout,
                storage.clone(),
                |_| {
                    let index = seen;
                    seen += 1;
                    if index == stop {
                        Err(ProbeFailure::Request(stop))
                    } else {
                        Ok(())
                    }
                },
                |_| Ok(()),
            );
            assert!(matches!(rejected, Err(ProbeFailure::Request(actual)) if actual == stop));
            assert_eq!(seen, stop + 1);
        }
        let mut request_count = 0;
        let refused_work = ChunkSchema::from_compiled_layout_owned_observed::<ProbeFailure>(
            &layout,
            storage.clone(),
            |_| {
                request_count += 1;
                Ok(())
            },
            |_| Err(ProbeFailure::Work),
        );
        assert!(matches!(refused_work, Err(ProbeFailure::Work)));
        assert_eq!(request_count, 0);
        assert!(restored.is_typed());
        assert!(Arc::ptr_eq(restored.slots()[0].field_ref(), &field));
        assert_eq!(restored.slots()[0].unique_id(), Some(91));
        let retained_binding = restored.slots()[0].typed_binding().clone();
        let retained_child = retained_binding
            .value_type()
            .child_type_at(0)
            .unwrap()
            .clone();
        assert_eq!(retained_child.domain(), Some(ValueLogicalType::Json));
        drop(layout);
        drop(field);
        drop(storage);
        drop(restored);
        drop(static_restored);
        drop(funded_restored);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert_eq!(retained_binding.layout().data_type(), &carrier);
        drop(retained_binding);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(retained_child);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

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
        assert!(err.to_string().contains("duplicate slot id"), "err={}", err);
    }

    #[test]
    fn schema_and_field_metadata_survive_slot_binding() {
        let schema = Schema::new_with_metadata(
            vec![
                Field::new("value", DataType::Int32, false)
                    .with_metadata(HashMap::from([("field".to_string(), "exact".to_string())])),
            ],
            HashMap::from([("schema".to_string(), "exact".to_string())]),
        );
        let chunk = ChunkSchema::try_ref_from_schema_and_slot_ids(&schema, &[SlotId::new(7)])
            .expect("bind schema");
        assert_eq!(chunk.arrow_schema_ref().as_ref(), &schema);
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
        assert_eq!(
            slot.field_schema().logical_type(),
            Some(ValueLogicalType::Hll)
        );
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

        assert_eq!(chunk.record_batch().num_columns(), 0);
        assert_eq!(chunk.record_batch().num_rows(), 3);
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
    fn nullable_map_keys_are_rejected_before_alignment_by_sdk_and_typed_slot() {
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
        let actual_type = DataType::Map(
            Arc::new(Field::new("entries", entries.data_type().clone(), false)),
            false,
        );
        let error = MapArray::try_new(
            Arc::new(Field::new("entries", entries.data_type().clone(), false)),
            OffsetBuffer::new(vec![0, 2].into()),
            entries,
            None,
            false,
        )
        .unwrap_err();
        assert!(matches!(
            error,
            arrow::error::ArrowError::InvalidArgumentError(_)
        ));
        assert!(error.to_string().contains("keys field cannot be nullable"));

        // The former alignment fixture cannot be represented by a safe
        // MapArray. Exercise the original checked slot author directly;
        // align_chunk_schema_to_columns ultimately rebuilds this same slot.
        let error = schema.slots()[0]
            .with_field_and_slot_id(SlotId::new(1), Field::new("m", actual_type, false))
            .unwrap_err();
        assert!(error.to_string().contains("map key type is nullable"));
        assert_eq!(
            novarocks_failure_contract::FailureProjection::failure_fact(&error).class(),
            novarocks_failure_contract::FailureClass::Invariant,
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
        assert!(
            err.to_string().contains("chunk schema type mismatch"),
            "err={err}"
        );
        assert!(err.to_string().contains("slot 9 (payload)"), "err={err}");
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
        assert!(
            err.to_string().contains("chunk schema type mismatch"),
            "err={err}"
        );
        assert!(err.to_string().contains("slot 9 (payload)"), "err={err}");
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

        assert!(
            err.to_string().contains("chunk schema type mismatch"),
            "err={err}"
        );
        assert!(err.to_string().contains("slot 9 (payload)"), "err={err}");
        assert!(err.to_string().contains("Binary"), "err={err}");
        assert!(err.to_string().contains("Utf8"), "err={err}");
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

        assert!(
            err.to_string().contains("chunk schema type mismatch"),
            "err={err}"
        );
        assert!(err.to_string().contains("slot 11 (price)"), "err={err}");
        assert!(err.to_string().contains("Decimal128(38, 2)"), "err={err}");
        assert!(err.to_string().contains("Decimal128(10, 2)"), "err={err}");
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

        assert!(err.to_string().contains("slot 10 (ts)"), "err={err}");
        assert!(
            err.to_string().contains("Timestamp(Nanosecond, None)"),
            "err={err}"
        );
    }
}

// Numerical resource provenance is deliberately absent from semantic equality
// and original Debug output. It cannot change a Chunk's field contract.
impl std::fmt::Debug for ChunkSchema {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        out.debug_struct("ChunkSchema")
            .field("slots", &self.slots)
            .field("arrow_schema", &self.arrow_schema)
            .field("slot_ids", &self.slot_ids)
            .field("index_by_slot", &self.index_by_slot)
            .finish()
    }
}
impl PartialEq for ChunkSchema {
    fn eq(&self, other: &Self) -> bool {
        self.slots == other.slots
            && self.arrow_schema == other.arrow_schema
            && self.slot_ids == other.slot_ids
            && self.index_by_slot == other.index_by_slot
    }
}
impl Eq for ChunkSchema {}
enum ChunkMetadataSource {
    Owned(ArrowMetadataOwner),
    Derived(MetadataOwnedSchema),
    Foreign(arrow::datatypes::Metadata),
    Original(OriginalMetadataClone),
    EmptyOriginal(MaterializedMetadataMap),
}

#[cfg(test)]
#[path = "metadata_materialization_tests.rs"]
mod metadata_materialization_tests;

#[cfg(test)]
#[path = "metadata_request.rs"]
mod metadata_request;

#[cfg(test)]
mod merged_metadata_origin_tests {
    use super::*;

    #[test]
    fn merged_metadata_origin_shared_m07_field_preserves_exact_arc_without_uea_fact() {
        let owner = ArrowMetadataOwner::try_new(
            vec![("PARQUET:field_id".into(), "9".into())],
            MetadataOwnerLimits {
                entries: 1,
                construction_bytes: 4096,
            },
        )
        .unwrap()
        .into_field("k".into(), DataType::Int64, false);
        let field = Arc::clone(owner.field());
        let origins = FieldMetadataOrigins::try_new(vec![owner], 1).unwrap();
        let slot = ChunkSlotSchema::try_new_with_metadata_origins(
            SlotId::new(9),
            Arc::clone(&field),
            origins,
            None,
            None,
        )
        .unwrap();
        let copied = slot.clone();
        assert!(Arc::ptr_eq(copied.field_ref(), &field));
        assert!(copied.field.loan().is_none());
        assert!(
            copied
                .metadata_origins()
                .unwrap()
                .metadata_bytes_for(&field)
                .is_some()
        );
        let equal_foreign = Arc::new(field.as_ref().clone());
        assert!(
            copied
                .metadata_origins()
                .unwrap()
                .metadata_bytes_for(&equal_foreign)
                .is_none()
        );
    }

    #[test]
    fn merged_metadata_origin_uea_slot_and_schema_publications_have_their_own_exact_loans() {
        let mut metadata = MaterializedMetadataMap::with_capacity(17);
        metadata.insert("PARQUET:field_id".into(), "9".into());
        let slot = ChunkSlotSchema::try_new_owned_field(
            SlotId::new(9),
            OriginalFieldMaterialization::Materialized(metadata.into_field(Field::new(
                "k",
                DataType::Int64,
                false,
            ))),
            None,
            None,
        )
        .unwrap();
        assert!(slot.field.loan().unwrap().lends(slot.field_ref()));
        let schema = ChunkSchema::try_new_with_metadata_source(
            vec![slot],
            ChunkMetadataSource::EmptyOriginal(MaterializedMetadataMap::new_for_original_reserve()),
            Some(&[]),
        )
        .unwrap();
        let actual = &schema.arrow_schema.fields()[0];
        assert!(!Arc::ptr_eq(actual, schema.slots[0].field_ref()));
        assert!(
            schema
                .metadata_materializations()
                .unwrap()
                .field_origin(actual)
                .is_some()
        );
        let foreign = Arc::new(actual.as_ref().clone());
        assert!(
            schema
                .metadata_materializations()
                .unwrap()
                .field_origin(&foreign)
                .is_none()
        );
        assert!(schema.field_metadata_origins().is_none());
    }
}
