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

//! Positive metadata-clone facts from the original insertion-only constructor.
//! An arbitrary HashMap capacity, Field size, or equal metadata is not an origin.
//! This owns no allocator, MEM grant, account, observation, or execution policy.
use super::hashmap::{FreshTableFacts, HashMapResourceError, fresh_table_layout};
use arrow_schema::{Field, FieldRef, Fields, Metadata, Schema, SchemaRef};
use std::{
    collections::{HashMap, TryReserveError},
    sync::{Arc, Weak},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetadataMaterializationError {
    Model(HashMapResourceError),
    BTree(super::btree::BTreeResourceError),
    ValueType(crate::ValueTypeError),
    InsertCountExceededOriginalReservation,
    MissingOriginalFieldOrigin,
    Arithmetic,
}

impl From<crate::ValueTypeError> for MetadataMaterializationError {
    fn from(error: crate::ValueTypeError) -> Self {
        Self::ValueType(error)
    }
}

/// Borrow ONE actual original allocation request before any host-specific
/// attribution band or admission. Repetitions name original clone occurrences;
/// they do not merge several small requests into one tagged allocation.
/// This callback neither allocates, grants, nor publishes an allocator fact.
pub type MetadataAllocationLoan<'a, E> =
    &'a mut dyn FnMut(std::alloc::Layout, usize) -> Result<(), E>;
fn allocation<E>(
    loan: &mut Option<MetadataAllocationLoan<'_, E>>,
    layout: std::alloc::Layout,
    occurrences: usize,
) -> Result<(), E> {
    if layout.size() != 0
        && occurrences != 0
        && let Some(loan) = loan.as_deref_mut()
    {
        loan(layout, occurrences)?;
    }
    Ok(())
}
fn metadata_model_error(error: super::btree::BTreeResourceError) -> MetadataMaterializationError {
    MetadataMaterializationError::BTree(error)
}

fn text_layout<E: From<MetadataMaterializationError>>(
    bytes: usize,
) -> Result<std::alloc::Layout, E> {
    std::alloc::Layout::array::<u8>(bytes)
        .map_err(|_| E::from(MetadataMaterializationError::Arithmetic))
}

/// ONE original Field clone's requests. Shared child Field/Fields/UnionFields
/// and timezone Arc payloads make no new requests. The original Dictionary
/// boxes recurse through the existing borrowed source grammar. The table is
/// paired with this actual Field owner, never inferred from map capacity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OriginalFieldCloneRequest {
    pub name_bytes: usize,
    pub metadata: MetadataTableCloneRequest,
    pub dictionary_box_requests: usize,
    pub dictionary_box_bytes: usize,
    pub request_bytes: usize,
}
fn original_field_clone_request<
    'a,
    E: From<MetadataMaterializationError> + From<crate::ValueTypeError>,
>(
    field: &'a Field,
    origin: MetadataCloneOrigin,
    scratch: &mut crate::owned_resources::type_validation::TypeValidationScratch<'a>,
    observe: &mut impl FnMut() -> Result<(), E>,
) -> Result<OriginalFieldCloneRequest, E> {
    original_field_clone_request_core(field, origin, scratch, observe, &mut None)
}

fn original_field_clone_request_core<
    'a,
    E: From<MetadataMaterializationError> + From<crate::ValueTypeError>,
>(
    field: &'a Field,
    origin: MetadataCloneOrigin,
    scratch: &mut crate::owned_resources::type_validation::TypeValidationScratch<'a>,
    observe: &mut impl FnMut() -> Result<(), E>,
    loan: &mut Option<MetadataAllocationLoan<'_, E>>,
) -> Result<OriginalFieldCloneRequest, E> {
    let metadata = original_map_clone_request_core(origin, field.metadata(), observe, loan)?;
    let mut dictionary_box_requests = 0_usize;
    let mut dictionary_box_bytes = 0_usize;
    crate::visit_original_data_type_clone_with_scratch_observed(
        field.data_type(),
        scratch,
        |visit| {
            if let crate::ValueTypeVisit::TypeNode(arrow_schema::DataType::Dictionary(_, _)) = visit
            {
                allocation(loan, std::alloc::Layout::new::<arrow_schema::DataType>(), 2)?;
                dictionary_box_requests = dictionary_box_requests
                    .checked_add(2)
                    .ok_or_else(|| E::from(MetadataMaterializationError::Arithmetic))?;
                dictionary_box_bytes = dictionary_box_bytes
                    .checked_add(2 * std::mem::size_of::<arrow_schema::DataType>())
                    .ok_or_else(|| E::from(MetadataMaterializationError::Arithmetic))?;
            }
            observe()
        },
    )?;
    allocation(loan, text_layout::<E>(field.name().len())?, 1)?;
    let request_bytes = field
        .name()
        .len()
        .checked_add(metadata.request_bytes().map_err(E::from)?)
        .and_then(|n| n.checked_add(dictionary_box_bytes))
        .ok_or_else(|| E::from(MetadataMaterializationError::Arithmetic))?;
    Ok(OriginalFieldCloneRequest {
        name_bytes: field.name().len(),
        metadata,
        dictionary_box_requests,
        dictionary_box_bytes,
        request_bytes,
    })
}

/// Allocation requests of ONE original immutable map clone. These are a
/// numerical caller contribution, not an operation grant or complete Field
/// clone extent: the latter also owns its name and DataType clone occurrences.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetadataTableCloneRequest {
    pub table_backing: Option<std::alloc::Layout>,
    pub text_requests: usize,
    pub text_bytes: usize,
}
impl MetadataTableCloneRequest {
    pub fn request_bytes(self) -> Result<usize, MetadataMaterializationError> {
        self.table_backing
            .map_or(0, |layout| layout.size())
            .checked_add(self.text_bytes)
            .ok_or(MetadataMaterializationError::Arithmetic)
    }
}

// Live backing is distinct from a clone request: immutable Metadata retains
// its Arc/BTree and actual String capacities while clone requests remain zero.
fn original_map_backing_observed<E: From<MetadataMaterializationError>>(
    origin: MetadataCloneOrigin,
    values: &Metadata,
    observe: &mut impl FnMut() -> Result<(), E>,
) -> Result<usize, E> {
    // The original staging map proves this paired immutable owner, but its
    // buckets no longer describe Arrow 60's attached Arc<BTreeMap> backing.
    origin.table_request().map_err(E::from)?;
    let mut bytes = super::arrow_metadata::hashmap_conversion_requests(values.len())
        .and_then(|requests| requests.retained_backing_upper_bound())
        .map_err(metadata_model_error)
        .map_err(E::from)?;
    observe()?;
    for (key, value) in values {
        bytes = bytes
            .checked_add(key.capacity())
            .and_then(|v| v.checked_add(value.capacity()))
            .ok_or_else(|| E::from(MetadataMaterializationError::Arithmetic))?;
        observe()?;
    }
    Ok(bytes)
}

// Sort the existing freshly published loan buffer in place. No allocation,
// map, payload clone, deduplication or metadata receipt conversion occurs.
// Shared tables are immutable; a previously published sorted table stays so.
fn index_original_loans(mut fields: Arc<[MetadataFieldLoan]>) -> Arc<[MetadataFieldLoan]> {
    if let Some(unique) = Arc::get_mut(&mut fields) {
        unique.sort_unstable_by_key(MetadataFieldLoan::original_address);
    }
    fields
}

// Only an already-paired owner calls this private arithmetic. An arbitrary
// HashMap plus a copied numerical origin cannot mint a source pairing.
fn original_map_clone_request<E: From<MetadataMaterializationError>>(
    metadata: MetadataCloneOrigin,
    values: &Metadata,
    observe: &mut impl FnMut() -> Result<(), E>,
) -> Result<MetadataTableCloneRequest, E> {
    original_map_clone_request_core(metadata, values, observe, &mut None)
}

fn original_map_clone_request_core<E: From<MetadataMaterializationError>>(
    metadata: MetadataCloneOrigin,
    values: &Metadata,
    observe: &mut impl FnMut() -> Result<(), E>,
    loan: &mut Option<MetadataAllocationLoan<'_, E>>,
) -> Result<MetadataTableCloneRequest, E> {
    // Metadata::clone is Option<Arc<BTreeMap>>::clone in Arrow 60. It has
    // no table, Arc header or String allocation request. The live shared
    // backing remains a separate source obligation, never a fresh clone.
    metadata.table_request().map_err(E::from)?;
    let _ = (values, loan);
    observe()?;
    Ok(MetadataTableCloneRequest {
        table_backing: None,
        text_requests: 0,
        text_bytes: 0,
    })
}

/// The original HashMap::with_capacity and insertion occurrences live here.
/// No removal, mutable map escape, second reservation or table compaction can
/// invalidate its construction receipt. A failed numerical source projection
/// stays separate from the original successful value operation.
#[derive(Debug)]
pub struct MaterializedMetadataMap {
    values: HashMap<String, String>,
    original_reservation: usize,
    insertions: usize,
    reserved_once: bool,
    facts: Result<FreshTableFacts, MetadataMaterializationError>,
}
impl MaterializedMetadataMap {
    pub fn with_capacity(original_reservation: usize) -> Self {
        // Keep the original allocation and panic before numerical observation.
        let values = HashMap::with_capacity(original_reservation);
        let facts = fresh_table_layout::<String, String>(original_reservation)
            .map_err(MetadataMaterializationError::Model);
        Self {
            values,
            original_reservation,
            insertions: 0,
            reserved_once: true,
            facts,
        }
    }
    /// Preserve the original empty-map plus ONE fallible reservation author.
    /// A second reservation remains an explicitly unavailable resource fact;
    /// it never changes the original map's value or error operation.
    pub fn new_for_original_reserve() -> Self {
        Self {
            values: HashMap::new(),
            original_reservation: 0,
            insertions: 0,
            facts: fresh_table_layout::<String, String>(0)
                .map_err(MetadataMaterializationError::Model),
            reserved_once: false,
        }
    }
    pub fn try_reserve_original(&mut self, additional: usize) -> Result<(), TryReserveError> {
        let result = self.values.try_reserve(additional);
        if result.is_ok() {
            if self.reserved_once || self.insertions != 0 {
                self.facts =
                    Err(MetadataMaterializationError::InsertCountExceededOriginalReservation);
            } else {
                self.original_reservation = additional;
                self.facts = fresh_table_layout::<String, String>(additional)
                    .map_err(MetadataMaterializationError::Model);
            }
            self.reserved_once = true;
        }
        result
    }
    pub fn insert(&mut self, key: String, value: String) -> Option<String> {
        // Exactly the original map insertion, replacement result and Drop
        // timing; receiving a fact never chooses value semantics or capacity.
        let previous = self.values.insert(key, value);
        match self.insertions.checked_add(1) {
            Some(insertions) => {
                self.insertions = insertions;
                if insertions > self.original_reservation {
                    self.facts =
                        Err(MetadataMaterializationError::InsertCountExceededOriginalReservation);
                }
            }
            None => self.facts = Err(MetadataMaterializationError::Arithmetic),
        }
        previous
    }
    pub fn into_schema(self, fields: impl Into<Fields>) -> MaterializedSchema {
        MaterializedSchema {
            schema: Schema::new_with_metadata(fields, self.values),
            metadata: MetadataCloneOrigin { facts: self.facts },
            storage: crate::ValueTypeStorageLifetime::pure(),
        }
    }
    pub fn into_field(self, field: Field) -> MaterializedField {
        MaterializedField {
            field: field.with_metadata(self.values),
            metadata: MetadataCloneOrigin { facts: self.facts },
        }
    }
}

/// No public constructor accepts an arbitrary map or a fabricated Layout.
/// The source map is kept immutable inside its paired original Field owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetadataCloneOrigin {
    facts: Result<FreshTableFacts, MetadataMaterializationError>,
}
impl MetadataCloneOrigin {
    /// Original temporary HashMap request, before Arrow 60 converts it to
    /// immutable Metadata. This is a provenance fact about the staging author,
    /// not the attached BTree backing or a Metadata::clone allocation.
    pub fn table_request(self) -> Result<FreshTableFacts, MetadataMaterializationError> {
        self.facts
    }
}

#[derive(Debug)]
pub struct MaterializedField {
    field: Field,
    metadata: MetadataCloneOrigin,
}
impl MaterializedField {
    pub fn field(&self) -> &Field {
        &self.field
    }
    pub fn metadata_origin(&self) -> MetadataCloneOrigin {
        self.metadata
    }
    pub fn original_metadata_clone_request<E: From<MetadataMaterializationError>>(
        &self,
        observe: &mut impl FnMut() -> Result<(), E>,
    ) -> Result<MetadataTableCloneRequest, E> {
        original_map_clone_request(self.metadata, self.field.metadata(), observe)
    }
    pub fn original_field_clone_request<
        'a,
        E: From<MetadataMaterializationError> + From<crate::ValueTypeError>,
    >(
        &'a self,
        scratch: &mut crate::owned_resources::type_validation::TypeValidationScratch<'a>,
        observe: &mut impl FnMut() -> Result<(), E>,
    ) -> Result<OriginalFieldCloneRequest, E> {
        original_field_clone_request(&self.field, self.metadata, scratch, observe)
    }
    pub fn original_field_clone_allocation_requests_observed<
        'a,
        E: From<MetadataMaterializationError> + From<crate::ValueTypeError>,
    >(
        &'a self,
        scratch: &mut crate::owned_resources::type_validation::TypeValidationScratch<'a>,
        observe: &mut impl FnMut() -> Result<(), E>,
        loan: &mut Option<MetadataAllocationLoan<'_, E>>,
    ) -> Result<OriginalFieldCloneRequest, E> {
        original_field_clone_request_core(&self.field, self.metadata, scratch, observe, loan)
    }
    pub fn clone_original(&self) -> Self {
        Self {
            field: self.field.clone(),
            metadata: self.metadata,
        }
    }
    /// Preserve the original Arc constructor at its existing publication site.
    pub fn into_shared(self) -> SharedMaterializedField {
        SharedMaterializedField {
            field: Arc::new(self.field),
            metadata: self.metadata,
        }
    }
    /// Explicitly leaving the resource-receipt chain grants nothing. Original
    /// value-only callers may retain their existing plain Field API.
    pub fn into_original_field(self) -> Field {
        self.field
    }
    pub fn with_nullable_original(&self, nullable: bool) -> Self {
        Self {
            field: self.field.clone().with_nullable(nullable),
            metadata: self.metadata,
        }
    }
    pub fn rebuild_original(&self, data_type: arrow_schema::DataType, nullable: bool) -> Self {
        Self {
            field: rebuild_original_field(&self.field, data_type, nullable),
            metadata: self.metadata,
        }
    }
}
#[derive(Clone, Debug)]
pub struct SharedMaterializedField {
    field: FieldRef,
    metadata: MetadataCloneOrigin,
}
impl SharedMaterializedField {
    pub fn field(&self) -> &FieldRef {
        &self.field
    }
    pub fn metadata_origin(&self) -> MetadataCloneOrigin {
        self.metadata
    }
    pub fn loan(&self) -> MetadataFieldLoan {
        MetadataFieldLoan {
            field: Arc::downgrade(&self.field),
            metadata: self.metadata,
        }
    }
    pub fn into_original_field_ref(self) -> FieldRef {
        self.field
    }
    pub fn lends(&self, actual: &FieldRef) -> bool {
        Arc::ptr_eq(&self.field, actual)
    }
    pub fn clone_original(&self) -> MaterializedField {
        MaterializedField {
            field: self.field.as_ref().clone(),
            metadata: self.metadata,
        }
    }
}

/// The original source's schema metadata is paired before it can escape. This
/// does not certify the schema fields' independent construction origins.
#[derive(Debug)]
pub struct MaterializedSchema {
    schema: Schema,
    metadata: MetadataCloneOrigin,
    // Original Fields/root-map payload exits before its finite responsibility.
    storage: crate::ValueTypeStorageLifetime,
}
impl MaterializedSchema {
    /// Attach the actual completed finite Schema/Field loan, never authority.
    pub fn with_storage_lifetime(mut self, storage: crate::ValueTypeStorageLifetime) -> Self {
        self.storage = storage;
        self
    }
    pub fn storage_lifetime(&self) -> crate::ValueTypeStorageLifetime {
        self.storage.clone()
    }
    pub fn schema(&self) -> &Schema {
        &self.schema
    }
    pub fn metadata_origin(&self) -> MetadataCloneOrigin {
        self.metadata
    }
    pub fn into_shared(self) -> SharedMaterializedSchema {
        SharedMaterializedSchema {
            schema: Arc::new(self.schema),
            metadata: self.metadata,
            storage: self.storage,
        }
    }
    pub fn into_original_schema(self) -> Schema {
        self.schema
    }
}
#[derive(Clone, Debug)]
pub struct SharedMaterializedSchema {
    schema: SchemaRef,
    metadata: MetadataCloneOrigin,
    storage: crate::ValueTypeStorageLifetime,
}
impl SharedMaterializedSchema {
    pub fn storage_lifetime(&self) -> crate::ValueTypeStorageLifetime {
        self.storage.clone()
    }
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }
    pub fn metadata_origin(&self) -> MetadataCloneOrigin {
        self.metadata
    }
    pub fn into_original_schema_ref(self) -> SchemaRef {
        self.schema
    }
    pub fn clone_original(&self) -> MaterializedSchema {
        MaterializedSchema {
            schema: self.schema.as_ref().clone(),
            metadata: self.metadata,
            storage: self.storage.clone(),
        }
    }
    /// Clone the original table before its caller's existing slot/index loop.
    /// The numerical receipt follows the clone without reconstructing a map.
    pub fn original_metadata_clone_request<E: From<MetadataMaterializationError>>(
        &self,
        observe: &mut impl FnMut() -> Result<(), E>,
    ) -> Result<MetadataTableCloneRequest, E> {
        original_map_clone_request(self.metadata, self.schema.metadata(), observe)
    }
    pub fn clone_metadata_original(&self) -> OriginalMetadataClone {
        OriginalMetadataClone {
            values: self.schema.metadata().clone(),
            metadata: self.metadata,
        }
    }
    pub fn lends(&self, actual: &SchemaRef) -> bool {
        Arc::ptr_eq(&self.schema, actual)
    }
    /// Actual live text capacities and the original table construction history.
    /// The supplied Schema must be this same immutable Arc, not equal content.
    pub fn original_backing_observed<E: From<MetadataMaterializationError>>(
        &self,
        actual: &SchemaRef,
        observe: &mut impl FnMut() -> Result<(), E>,
    ) -> Option<Result<usize, E>> {
        self.lends(actual)
            .then(|| original_map_backing_observed(self.metadata, actual.metadata(), observe))
    }
}

/// Call the ONE original FVT materializer, then record its explicit root-map
/// construction branch. No metadata iteration, capacity inversion, root type
/// inference or reconstructed Field substitutes for that source occurrence.
/// The legacy root map is new/empty, or made from the original one-entry array; its
/// nested DataType FieldRefs remain the original source's independent owners.
pub fn materialize_value_field(
    value: &crate::FunctionValueType,
    name: impl Into<String>,
) -> Result<MaterializedField, crate::ValueTypeError> {
    let field = value.try_to_field(name)?;
    let original_entries = usize::from(value.logical_type().metadata_value().is_some());
    Ok(MaterializedField {
        field,
        metadata: MetadataCloneOrigin {
            facts: fresh_table_layout::<String, String>(original_entries)
                .map_err(MetadataMaterializationError::Model),
        },
    })
}

#[cfg(test)]
mod value_field_author_tests {
    use super::materialize_value_field;
    use crate::{FunctionValueType, ValueLogicalType};
    use arrow_schema::DataType;

    #[test]
    fn legacy_value_field_receipt_matches_the_actual_source_author() {
        for domain in [ValueLogicalType::Json, ValueLogicalType::Hll] {
            let carrier = if domain == ValueLogicalType::Json {
                DataType::Utf8
            } else {
                DataType::Binary
            };
            let value = FunctionValueType::try_with_logical_type(carrier, true, domain).unwrap();
            let field = materialize_value_field(&value, "source").unwrap();
            assert_eq!(field.field().metadata().len(), 1);
            assert_eq!(
                field
                    .field()
                    .metadata()
                    .get(crate::NR_LOGICAL_TYPE_KEY)
                    .map(String::as_str),
                domain.metadata_value(),
            );
            assert_eq!(value.logical_type(), domain);
            let original = field.metadata_origin().table_request().unwrap();
            assert!(original.layout.is_some());
            assert_eq!(original.allocation_requests_upper_bound, 1);
            assert!(original.request_bytes_upper_bound > 0);
            let request = field
                .original_metadata_clone_request(&mut || {
                    Ok::<(), super::MetadataMaterializationError>(())
                })
                .unwrap();
            assert_eq!(request.table_backing, None);
            assert_eq!((request.text_requests, request.text_bytes), (0, 0));
            assert_eq!(request.request_bytes().unwrap(), 0);
            let copied = field.clone_original();
            assert!(std::sync::Arc::ptr_eq(
                field.field().metadata().as_arc().unwrap(),
                copied.field().metadata().as_arc().unwrap()
            ));
            assert_eq!(
                field.clone_original().metadata_origin(),
                field.metadata_origin()
            );
        }
        let physical = FunctionValueType::new(DataType::Utf8, true);
        let field = materialize_value_field(&physical, "source").unwrap();
        assert!(field.field().metadata().is_empty());
        assert_eq!(
            field.metadata_origin().table_request().unwrap().layout,
            None
        );
    }
}

/// ONE original Chunk field reconstruction. Dictionary attributes retain its
/// original Field::new behavior. This is neither a carrier conversion nor a
/// source receipt constructor for an arbitrary foreign metadata table.
pub fn rebuild_original_field(
    expected: &Field,
    data_type: arrow_schema::DataType,
    nullable: bool,
) -> Field {
    Field::new(expected.name(), data_type, nullable).with_metadata(expected.metadata().clone())
}

/// A source-owned collection of positive construction occurrences. It claims
/// only the exact owners present in the set; absence is not an empty/fresh map
/// proof. The source visitor supplies every original nested occurrence.
#[derive(Clone, Debug)]
pub struct SchemaMetadataMaterializations {
    schema: SharedMaterializedSchema,
    fields: Arc<[MetadataFieldLoan]>,
}
impl SchemaMetadataMaterializations {
    pub fn from_materialized_owners(
        schema: SharedMaterializedSchema,
        fields: Arc<[MetadataFieldLoan]>,
    ) -> Self {
        Self {
            schema,
            fields: index_original_loans(fields),
        }
    }
    pub fn schema_owner(&self) -> &SharedMaterializedSchema {
        &self.schema
    }
    /// Clone only the positively paired root metadata table. The supplied
    /// FieldRefs remain the original immutable owners, so inherited exact
    /// field loans keep their identity without a second field clone.
    pub fn project_shared_fields_original(&self, fields: impl Into<Fields>) -> Self {
        Self {
            schema: self
                .schema
                .clone_metadata_original()
                .into_schema(fields)
                .into_shared(),
            fields: Arc::clone(&self.fields),
        }
    }
    pub fn fields(&self) -> &[MetadataFieldLoan] {
        &self.fields
    }
    /// Borrow the exact source buffer only after its immutable ordering check.
    /// An externally shared, unsorted table is not a checked lookup index.
    pub fn checked_field_index(&self) -> Option<&[MetadataFieldLoan]> {
        self.fields
            .is_sorted_by_key(MetadataFieldLoan::original_address)
            .then_some(&self.fields)
    }
    pub fn field_namespace(&self) -> MaterializedFieldNamespace {
        MaterializedFieldNamespace {
            fields: self.fields.clone(),
            _storage: crate::ValueTypeStorageLifetime::pure(),
        }
    }
    pub fn field_origin(&self, field: &FieldRef) -> Option<MetadataCloneOrigin> {
        self.fields
            .iter()
            .find(|entry| entry.lends(field))
            .map(|entry| entry.metadata_origin())
    }
    /// Borrow only the exact retained owner. Every inspected namespace entry
    /// belongs to the caller's current operation; a refusal has no footer.
    pub fn field_loan_observed<E>(
        &self,
        field: &FieldRef,
        observe: &mut impl FnMut() -> Result<(), E>,
    ) -> Result<Option<&MetadataFieldLoan>, E> {
        for entry in self.fields.iter() {
            let same = entry.lends(field);
            observe()?;
            if same {
                return Ok(Some(entry));
            }
        }
        Ok(None)
    }
    pub fn field_origin_observed<E>(
        &self,
        field: &FieldRef,
        observe: &mut impl FnMut() -> Result<(), E>,
    ) -> Result<Option<MetadataCloneOrigin>, E> {
        self.field_loan_observed(field, observe)
            .map(|loan| loan.map(MetadataFieldLoan::metadata_origin))
    }
    /// A real original Schema clone shares its original FieldRefs and clones
    /// exactly the paired root table. No metadata iteration rematerializes it.
    pub fn original_borrowed_field_clone_request_observed<
        'a,
        E: From<MetadataMaterializationError> + From<crate::ValueTypeError>,
    >(
        &self,
        actual: &'a Field,
        scratch: &mut crate::owned_resources::type_validation::TypeValidationScratch<'a>,
        observe: &mut impl FnMut() -> Result<(), E>,
    ) -> Result<Option<OriginalFieldCloneRequest>, E> {
        self.original_borrowed_field_clone_allocation_requests_observed(
            actual, scratch, observe, &mut None,
        )
    }
    pub fn original_borrowed_field_clone_allocation_requests_observed<
        'a,
        E: From<MetadataMaterializationError> + From<crate::ValueTypeError>,
    >(
        &self,
        actual: &'a Field,
        scratch: &mut crate::owned_resources::type_validation::TypeValidationScratch<'a>,
        observe: &mut impl FnMut() -> Result<(), E>,
        allocations: &mut Option<MetadataAllocationLoan<'_, E>>,
    ) -> Result<Option<OriginalFieldCloneRequest>, E> {
        for loan in self.fields.iter() {
            let same = loan.lends_borrowed_field(actual);
            observe()?;
            if same {
                return loan
                    .original_borrowed_field_clone_allocation_requests_observed(
                        actual,
                        scratch,
                        observe,
                        allocations,
                    )
                    .expect("exact borrowed original Field loan")
                    .map(Some);
            }
        }
        Ok(None)
    }
    pub fn clone_original_schema(&self) -> Self {
        Self {
            schema: self.schema.clone_original().into_shared(),
            fields: self.fields.clone(),
        }
    }
}

/// Exact source identity without retaining a removed projection's Field
/// payload. Its original Arc allocation header stays pinned until the last
/// weak loan drops; this header is a real retained resource, not free stock.
#[derive(Clone, Debug)]
pub struct MetadataFieldLoan {
    field: Weak<Field>,
    metadata: MetadataCloneOrigin,
}
impl MetadataFieldLoan {
    /// Exact allocation address pinned by this Weak, never an access grant.
    pub fn original_address(&self) -> usize {
        self.field.as_ptr() as usize
    }
    /// A temporary strong loan is available only while the original payload
    /// lives. Expired Weak receipts cannot expose destroyed name/map heaps.
    pub fn borrow_original_field(&self) -> Option<FieldRef> {
        self.field.upgrade()
    }
    pub fn original_backing_observed<E: From<MetadataMaterializationError>>(
        &self,
        actual: &FieldRef,
        observe: &mut impl FnMut() -> Result<(), E>,
    ) -> Option<Result<usize, E>> {
        self.lends(actual)
            .then(|| original_map_backing_observed(self.metadata, actual.metadata(), observe))
    }
    pub fn lends_borrowed_field(&self, actual: &Field) -> bool {
        std::ptr::eq(self.field.as_ptr(), actual)
    }
    pub fn original_borrowed_field_clone_request<
        'a,
        E: From<MetadataMaterializationError> + From<crate::ValueTypeError>,
    >(
        &self,
        actual: &'a Field,
        scratch: &mut crate::owned_resources::type_validation::TypeValidationScratch<'a>,
        observe: &mut impl FnMut() -> Result<(), E>,
    ) -> Option<Result<OriginalFieldCloneRequest, E>> {
        self.lends_borrowed_field(actual)
            .then(|| original_field_clone_request(actual, self.metadata, scratch, observe))
    }
    pub fn original_borrowed_field_clone_allocation_requests_observed<
        'a,
        E: From<MetadataMaterializationError> + From<crate::ValueTypeError>,
    >(
        &self,
        actual: &'a Field,
        scratch: &mut crate::owned_resources::type_validation::TypeValidationScratch<'a>,
        observe: &mut impl FnMut() -> Result<(), E>,
        loan: &mut Option<MetadataAllocationLoan<'_, E>>,
    ) -> Option<Result<OriginalFieldCloneRequest, E>> {
        self.lends_borrowed_field(actual).then(|| {
            original_field_clone_request_core(actual, self.metadata, scratch, observe, loan)
        })
    }
    pub fn lends(&self, actual: &FieldRef) -> bool {
        // The retained Weak pins this exact allocation header against reuse.
        // Compare only addresses; no payload dereference or temporary Weak.
        self.field.as_ptr() == Arc::as_ptr(actual)
    }
    pub fn metadata_origin(&self) -> MetadataCloneOrigin {
        self.metadata
    }
    pub fn original_metadata_clone_request<E: From<MetadataMaterializationError>>(
        &self,
        actual: &FieldRef,
        observe: &mut impl FnMut() -> Result<(), E>,
    ) -> Option<Result<MetadataTableCloneRequest, E>> {
        self.lends(actual)
            .then(|| original_map_clone_request(self.metadata, actual.metadata(), observe))
    }
    pub fn original_field_clone_request<
        'a,
        E: From<MetadataMaterializationError> + From<crate::ValueTypeError>,
    >(
        &self,
        actual: &'a FieldRef,
        scratch: &mut crate::owned_resources::type_validation::TypeValidationScratch<'a>,
        observe: &mut impl FnMut() -> Result<(), E>,
    ) -> Option<Result<OriginalFieldCloneRequest, E>> {
        self.lends(actual)
            .then(|| original_field_clone_request(actual, self.metadata, scratch, observe))
    }
    pub fn clone_original(&self, actual: &FieldRef) -> Option<MaterializedField> {
        self.lends(actual).then(|| MaterializedField {
            field: actual.as_ref().clone(),
            metadata: self.metadata,
        })
    }
}

/// The original projection supplies one already-cloned Field occurrence.
/// A foreign value remains an explicit absence of a clone source receipt.
#[derive(Debug)]
pub enum ProjectedMaterializedField {
    Original(MaterializedField),
    Foreign(Field),
}
impl SchemaMetadataMaterializations {
    pub fn clone_field_original_observed<E>(
        &self,
        field: &FieldRef,
        observe: &mut impl FnMut() -> Result<(), E>,
    ) -> Result<ProjectedMaterializedField, E> {
        Ok(match self.field_loan_observed(field, observe)? {
            Some(loan) => ProjectedMaterializedField::Original(
                loan.clone_original(field)
                    .expect("exact original Field loan"),
            ),
            None => ProjectedMaterializedField::Foreign(field.as_ref().clone()),
        })
    }
    pub fn clone_field_original(&self, field: &FieldRef) -> ProjectedMaterializedField {
        match self.fields.iter().find(|entry| entry.lends(field)) {
            Some(entry) => ProjectedMaterializedField::Original(
                entry
                    .clone_original(field)
                    .expect("exact original Field loan"),
            ),
            None => ProjectedMaterializedField::Foreign(field.as_ref().clone()),
        }
    }
    /// ONE original Schema projection constructor. The source owns its exact
    /// root map; child references are the original Field-clone products. No
    /// metadata iteration, compaction or capacity inversion happens here.
    /// The caller must admit both source receipt-vector copies and new root
    /// loans before calling this allocating materialization operation.
    pub fn project_original_schema(&self, projected: Vec<ProjectedMaterializedField>) -> Self {
        let mut fields = Vec::with_capacity(projected.len());
        let mut origins = Vec::with_capacity(projected.len());
        for field in projected {
            match field {
                ProjectedMaterializedField::Original(field) => {
                    origins.push(Some(field.metadata));
                    fields.push(field.field);
                }
                ProjectedMaterializedField::Foreign(field) => {
                    origins.push(None);
                    fields.push(field);
                }
            }
        }
        let schema = Arc::new(Schema::new_with_metadata(
            fields,
            self.schema.schema.metadata().clone(),
        ));
        let mut loans = Vec::with_capacity(self.fields.len() + origins.len());
        loans.extend(self.fields.iter().cloned());
        for (field, origin) in schema.fields().iter().zip(origins) {
            if let Some(metadata) = origin {
                loans.push(MetadataFieldLoan {
                    field: Arc::downgrade(field),
                    metadata,
                });
            }
        }
        Self {
            schema: SharedMaterializedSchema {
                schema,
                metadata: self.schema.metadata,
                storage: self.schema.storage.clone(),
            },
            fields: index_original_loans(loans.into()),
        }
    }
}

/// A table cloned from its paired immutable Schema. Only the original owner
/// can construct this object; no metadata iterator or capacity inference is
/// accepted. The caller still owes admission before the original clone.
#[derive(Debug)]
pub struct OriginalMetadataClone {
    values: Metadata,
    metadata: MetadataCloneOrigin,
}
impl OriginalMetadataClone {
    pub fn into_schema(self, fields: impl Into<Fields>) -> MaterializedSchema {
        MaterializedSchema {
            schema: Schema::new_with_metadata(fields, self.values),
            metadata: self.metadata,
            storage: crate::ValueTypeStorageLifetime::pure(),
        }
    }
}

/// A published original Field and its optional positive construction loan.
/// Both are produced together; foreign sharing never invents a metadata fact.
#[derive(Clone, Debug)]
pub struct OriginalSharedField {
    field: FieldRef,
    loan: Option<MetadataFieldLoan>,
    shared_reference: bool,
}
impl PartialEq for OriginalSharedField {
    fn eq(&self, other: &Self) -> bool {
        self.field == other.field
    }
}
impl Eq for OriginalSharedField {}
impl OriginalSharedField {
    pub fn from_original(original: OriginalFieldMaterialization) -> Self {
        let (field, loan) = original.into_shared();
        Self {
            field,
            loan,
            shared_reference: false,
        }
    }
    pub fn from_shared(field: FieldRef, source: Option<&SchemaMetadataMaterializations>) -> Self {
        let loan = source
            .and_then(|source| source.fields().iter().find(|loan| loan.lends(&field)))
            .cloned();
        Self {
            field,
            loan,
            shared_reference: true,
        }
    }
    pub fn shares_original_reference(&self) -> bool {
        self.shared_reference
    }
    pub fn field(&self) -> &Field {
        &self.field
    }
    pub fn field_ref(&self) -> &FieldRef {
        &self.field
    }
    pub fn loan(&self) -> Option<&MetadataFieldLoan> {
        self.loan.as_ref()
    }
    pub fn rebuild_original(
        &self,
        data_type: arrow_schema::DataType,
        nullable: bool,
    ) -> OriginalFieldMaterialization {
        match &self.loan {
            Some(loan) => OriginalFieldMaterialization::Materialized(MaterializedField {
                field: rebuild_original_field(&self.field, data_type, nullable),
                metadata: loan.metadata,
            }),
            None => OriginalFieldMaterialization::Plain(rebuild_original_field(
                &self.field,
                data_type,
                nullable,
            )),
        }
    }
    pub fn clone_original(&self) -> OriginalFieldMaterialization {
        match &self.loan {
            Some(loan) => OriginalFieldMaterialization::Materialized(
                loan.clone_original(&self.field)
                    .expect("paired original shared Field loan"),
            ),
            None => OriginalFieldMaterialization::Plain(self.field.as_ref().clone()),
        }
    }
    pub fn original_field_clone_allocation_requests_observed<
        'a,
        E: From<MetadataMaterializationError> + From<crate::ValueTypeError>,
    >(
        &'a self,
        scratch: &mut crate::owned_resources::type_validation::TypeValidationScratch<'a>,
        observe: &mut impl FnMut() -> Result<(), E>,
        allocations: &mut Option<MetadataAllocationLoan<'_, E>>,
    ) -> Option<Result<OriginalFieldCloneRequest, E>> {
        self.loan.as_ref().and_then(|loan| {
            loan.original_borrowed_field_clone_allocation_requests_observed(
                self.field(),
                scratch,
                observe,
                allocations,
            )
        })
    }
}

/// Optional positive root provenance travels with the actual owned Field.
/// Plain callers retain the original Field behavior and expose no fact.
pub enum OriginalFieldMaterialization {
    Plain(Field),
    Materialized(MaterializedField),
}
impl std::fmt::Debug for OriginalFieldMaterialization {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self.field(), out)
    }
}
impl PartialEq for OriginalFieldMaterialization {
    fn eq(&self, other: &Self) -> bool {
        self.field() == other.field()
    }
}
impl Eq for OriginalFieldMaterialization {}
impl Clone for OriginalFieldMaterialization {
    fn clone(&self) -> Self {
        match self {
            Self::Plain(field) => Self::Plain(field.clone()),
            Self::Materialized(field) => Self::Materialized(field.clone_original()),
        }
    }
}
impl OriginalFieldMaterialization {
    pub fn field(&self) -> &Field {
        match self {
            Self::Plain(field) => field,
            Self::Materialized(field) => field.field(),
        }
    }
    pub fn metadata_origin(&self) -> Option<MetadataCloneOrigin> {
        match self {
            Self::Plain(_) => None,
            Self::Materialized(field) => Some(field.metadata_origin()),
        }
    }
    pub fn original_field_clone_request<
        'a,
        E: From<MetadataMaterializationError> + From<crate::ValueTypeError>,
    >(
        &'a self,
        scratch: &mut crate::owned_resources::type_validation::TypeValidationScratch<'a>,
        observe: &mut impl FnMut() -> Result<(), E>,
    ) -> Option<Result<OriginalFieldCloneRequest, E>> {
        match self {
            Self::Plain(_) => None,
            Self::Materialized(field) => Some(field.original_field_clone_request(scratch, observe)),
        }
    }
    pub fn original_field_clone_allocation_requests_observed<
        'a,
        E: From<MetadataMaterializationError> + From<crate::ValueTypeError>,
    >(
        &'a self,
        scratch: &mut crate::owned_resources::type_validation::TypeValidationScratch<'a>,
        observe: &mut impl FnMut() -> Result<(), E>,
        loan: &mut Option<MetadataAllocationLoan<'_, E>>,
    ) -> Option<Result<OriginalFieldCloneRequest, E>> {
        match self {
            Self::Plain(_) => None,
            Self::Materialized(field) => Some(
                field.original_field_clone_allocation_requests_observed(scratch, observe, loan),
            ),
        }
    }
    pub fn with_nullable_original(&self, nullable: bool) -> Self {
        match self {
            Self::Plain(field) => Self::Plain(field.clone().with_nullable(nullable)),
            Self::Materialized(field) => Self::Materialized(field.with_nullable_original(nullable)),
        }
    }
    pub fn rebuild_original(&self, data_type: arrow_schema::DataType, nullable: bool) -> Self {
        match self {
            Self::Plain(field) => Self::Plain(rebuild_original_field(field, data_type, nullable)),
            Self::Materialized(field) => {
                Self::Materialized(field.rebuild_original(data_type, nullable))
            }
        }
    }
    pub fn with_nullable_owned(self, nullable: bool) -> Self {
        match self {
            Self::Plain(field) => Self::Plain(field.with_nullable(nullable)),
            Self::Materialized(field) => Self::Materialized(MaterializedField {
                field: field.field.with_nullable(nullable),
                metadata: field.metadata,
            }),
        }
    }
    /// Move through the original owned Field::with_name call, with no clone.
    pub fn with_name_owned(self, name: impl Into<String>) -> Self {
        match self {
            Self::Plain(field) => Self::Plain(field.with_name(name)),
            Self::Materialized(field) => Self::Materialized(MaterializedField {
                field: field.field.with_name(name),
                metadata: field.metadata,
            }),
        }
    }
    pub fn into_original_field(self) -> Field {
        match self {
            Self::Plain(field) => field,
            Self::Materialized(field) => field.into_original_field(),
        }
    }
    pub fn into_shared(self) -> (FieldRef, Option<MetadataFieldLoan>) {
        match self {
            Self::Plain(field) => (Arc::new(field), None),
            Self::Materialized(field) => {
                let shared = field.into_shared();
                let loan = shared.loan();
                (shared.into_original_field_ref(), Some(loan))
            }
        }
    }
    pub fn clone_from_source(
        field: &FieldRef,
        source: Option<&SchemaMetadataMaterializations>,
    ) -> Self {
        match source.map(|source| source.clone_field_original(field)) {
            Some(ProjectedMaterializedField::Original(field)) => Self::Materialized(field),
            Some(ProjectedMaterializedField::Foreign(field)) => Self::Plain(field),
            None => Self::Plain(field.as_ref().clone()),
        }
    }
}

impl SchemaMetadataMaterializations {
    /// Rebuild one exact borrowed source occurrence through the original
    /// reconstruction body. This never traverses or reinterprets DataType.
    pub fn rebuild_field_original(
        &self,
        field: &FieldRef,
        data_type: arrow_schema::DataType,
        nullable: bool,
    ) -> OriginalFieldMaterialization {
        match self.fields.iter().find(|entry| entry.lends(field)) {
            Some(entry) => OriginalFieldMaterialization::Materialized(MaterializedField {
                field: rebuild_original_field(field, data_type, nullable),
                metadata: entry.metadata,
            }),
            None => OriginalFieldMaterialization::Plain(rebuild_original_field(
                field, data_type, nullable,
            )),
        }
    }
    pub fn with_additional_original_loans(&self, additional: Vec<MetadataFieldLoan>) -> Self {
        let mut fields = Vec::with_capacity(self.fields.len() + additional.len());
        fields.extend(self.fields.iter().cloned());
        fields.extend(additional);
        Self {
            schema: self.schema.clone(),
            fields: index_original_loans(fields.into()),
        }
    }
}

/// Additional backing of the original source-sidecar publication. The actual
/// count is authored before the original Vec/Box/Arc operations. Keeping all
/// request contributions live is a conservative coexistence envelope, including
/// the old namespace, roots, Vec trim and the final Arc slice simultaneously.
/// This covers sidecar backing only, not Field/DataType/map clone payloads.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetadataSidecarRequest {
    pub inherited_loans: usize,
    pub original_roots: usize,
    pub loans_upper_bound: usize,
    pub temporary_requests_bytes: usize,
    pub result_request_bytes: usize,
    pub coexistence_request_bytes: usize,
}
impl MetadataSidecarRequest {
    pub fn original_publication(
        inherited_loans: usize,
        original_roots: usize,
    ) -> Result<Self, MetadataMaterializationError> {
        Self::original_publication_allocation_requests_observed(
            inherited_loans,
            original_roots,
            &mut None,
        )
    }

    pub fn original_publication_allocation_requests_observed<
        E: From<MetadataMaterializationError>,
    >(
        inherited_loans: usize,
        original_roots: usize,
        loan: &mut Option<MetadataAllocationLoan<'_, E>>,
    ) -> Result<Self, E> {
        let result = (|| -> Result<(Self, [(std::alloc::Layout, usize); 8]), MetadataMaterializationError> {
        let loans_upper_bound = inherited_loans
            .checked_add(original_roots)
            .ok_or(MetadataMaterializationError::Arithmetic)?;
        let loans = std::alloc::Layout::array::<MetadataFieldLoan>(loans_upper_bound)
            .map_err(|_| MetadataMaterializationError::Arithmetic)?;
        let roots = std::alloc::Layout::array::<FieldRef>(original_roots)
            .map_err(|_| MetadataMaterializationError::Arithmetic)?;
        let sidecar_arc = super::layout::arc_layout(loans)
            .map_err(|_| MetadataMaterializationError::Arithmetic)?;
        let fields_arc = super::layout::arc_layout(roots)
            .map_err(|_| MetadataMaterializationError::Arithmetic)?;
        // Vec::with_capacity requests the original count. Vec -> Box may trim
        // unused foreign-root slots; the full original count bounds that trim.
        // Positive typed/inherited roots occupy their actual owned carrier,
        // not the smaller plain Field representation. Origins are retained
        // until publication; the separate FieldRef vector cannot reuse this
        // carrier allocation through in-place iterator specialization.
        let input_fields =
            std::alloc::Layout::array::<OriginalFieldMaterialization>(original_roots)
                .map_err(|_| MetadataMaterializationError::Arithmetic)?;
        let origins = std::alloc::Layout::array::<Option<MetadataCloneOrigin>>(original_roots)
            .map_err(|_| MetadataMaterializationError::Arithmetic)?;
        let temporary_requests_bytes = loans
            .size()
            .checked_mul(2)
            .and_then(|n| n.checked_add(roots.size()))
            .and_then(|n| n.checked_add(input_fields.size()))
            .and_then(|n| n.checked_add(origins.size()))
            .ok_or(MetadataMaterializationError::Arithmetic)?;
        let root_arcs = super::layout::arc_layout(std::alloc::Layout::new::<Field>())
            .map_err(|_| MetadataMaterializationError::Arithmetic)?
            .size()
            .checked_mul(original_roots)
            .ok_or(MetadataMaterializationError::Arithmetic)?;
        let schema_arc = super::layout::arc_layout(std::alloc::Layout::new::<Schema>())
            .map_err(|_| MetadataMaterializationError::Arithmetic)?
            .size();
        let result_request_bytes = sidecar_arc
            .size()
            .checked_add(fields_arc.size())
            .and_then(|n| n.checked_add(root_arcs))
            .and_then(|n| n.checked_add(schema_arc))
            .ok_or(MetadataMaterializationError::Arithmetic)?;
        let coexistence_request_bytes = temporary_requests_bytes
            .checked_add(result_request_bytes)
            .ok_or(MetadataMaterializationError::Arithmetic)?;
        Ok((Self {
            inherited_loans,
            original_roots,
            loans_upper_bound,
            temporary_requests_bytes,
            result_request_bytes,
            coexistence_request_bytes,
        }, [
            (loans, 2), (roots, 1), (input_fields, 1), (origins, 1),
            (sidecar_arc, 1), (fields_arc, 1),
            (super::layout::arc_layout(std::alloc::Layout::new::<Field>()).map_err(|_| MetadataMaterializationError::Arithmetic)?, original_roots),
            (super::layout::arc_layout(std::alloc::Layout::new::<Schema>()).map_err(|_| MetadataMaterializationError::Arithmetic)?, 1),
        ]))
        })().map_err(E::from)?;
        for (layout, occurrences) in result.1 {
            allocation(loan, layout, occurrences)?;
        }
        Ok(result.0)
    }
}

/// The decoder's same original sparse Field owners. This is a numerical
/// provenance loan, not a wire fact, host grant or function/runtime binding.
#[derive(Clone, Debug)]
pub struct MaterializedFieldNamespace {
    fields: Arc<[MetadataFieldLoan]>,
    // Only the actual receipt-buffer owner; this is not a Field/type table,
    // semantic authority or source invoice used as a capacity grant.
    _storage: crate::ValueTypeStorageLifetime,
}
impl MaterializedFieldNamespace {
    /// Concatenate these two actual source namespaces, never electing a
    /// source by field equality. The existing caller's preparation scope owns
    /// this additional table; this method does not claim a MEM grant.
    pub fn join_original_observed(
        &self,
        other: &Self,
        work: &mut crate::CompileCheckpoints<'_>,
    ) -> Result<Self, crate::CompileControlError> {
        let count = self
            .fields
            .len()
            .checked_add(other.fields.len())
            .ok_or(crate::CompileControlError::ResourceExhausted)?;
        work.flush()?;
        let mut fields = Vec::new();
        let reserved = fields.try_reserve_exact(count);
        if reserved.is_ok() {
            work.step()?;
        }
        crate::owned_resources::copy::reserve_exit::<crate::CompileControlError>(reserved, work)?;
        for source in [self, other] {
            for field in source.fields.iter() {
                fields.push(field.clone());
                work.step()?;
            }
        }
        work.flush()?;
        let result = Self::from_original_loans(fields.into());
        work.flush()?;
        Ok(result)
    }
    pub fn from_original_loans(fields: Arc<[MetadataFieldLoan]>) -> Self {
        Self {
            fields: index_original_loans(fields),
            _storage: crate::ValueTypeStorageLifetime::pure(),
        }
    }
    /// Couple an already-observed receipt publication to its actual finite
    /// owner. Moving this inline marker creates no allocation or authority.
    pub fn with_storage_lifetime(mut self, storage: crate::ValueTypeStorageLifetime) -> Self {
        self._storage = storage;
        self
    }
    pub fn fields(&self) -> &[MetadataFieldLoan] {
        &self.fields
    }
    pub fn original_publication_request(
        &self,
        roots: usize,
    ) -> Result<MetadataSidecarRequest, MetadataMaterializationError> {
        MetadataSidecarRequest::original_publication(self.fields.len(), roots)
    }
    pub fn original_field_clone_request_observed<
        'a,
        E: From<MetadataMaterializationError> + From<crate::ValueTypeError>,
    >(
        &self,
        actual: &'a FieldRef,
        scratch: &mut crate::owned_resources::type_validation::TypeValidationScratch<'a>,
        observe: &mut impl FnMut() -> Result<(), E>,
    ) -> Result<Option<OriginalFieldCloneRequest>, E> {
        for loan in self.fields.iter() {
            let same = loan.lends(actual);
            observe()?;
            if same {
                return loan
                    .original_field_clone_request(actual, scratch, observe)
                    .expect("exact original Field loan")
                    .map(Some);
            }
        }
        Ok(None)
    }
}

/// Materialize the original typed root Fields once, retaining their paired
/// metadata construction occurrence until the original Schema conversion.
/// All vectors/Arc metadata are real additional retained preparation backing;
/// the caller owes its admission before using this optional positive path.
pub struct TypedSchemaMaterializations {
    fields: OriginalSchemaFields,
    namespace: MaterializedFieldNamespace,
}
enum OriginalSchemaFields {
    Typed(Vec<MaterializedField>),
    Inherited(Vec<OriginalFieldMaterialization>),
}
impl TypedSchemaMaterializations {
    pub fn new(fields: Vec<MaterializedField>, namespace: MaterializedFieldNamespace) -> Self {
        Self {
            fields: OriginalSchemaFields::Typed(fields),
            namespace,
        }
    }
    pub fn from_original_fields(
        fields: Vec<OriginalFieldMaterialization>,
        namespace: MaterializedFieldNamespace,
    ) -> Self {
        Self {
            fields: OriginalSchemaFields::Inherited(fields),
            namespace,
        }
    }
    pub fn original_publication_request(
        &self,
    ) -> Result<MetadataSidecarRequest, MetadataMaterializationError> {
        let roots = match &self.fields {
            OriginalSchemaFields::Typed(fields) => fields.len(),
            OriginalSchemaFields::Inherited(fields) => fields.len(),
        };
        self.namespace.original_publication_request(roots)
    }
    pub fn into_original_schema_in<E: From<MetadataMaterializationError>>(
        self,
        admit: &mut impl FnMut(&MetadataSidecarRequest) -> Result<(), E>,
    ) -> Result<SchemaMetadataMaterializations, E> {
        let request = self.original_publication_request().map_err(E::from)?;
        admit(&request)?;
        Ok(self.into_original_schema())
    }
    pub fn into_original_schema(self) -> SchemaMetadataMaterializations {
        let roots = match &self.fields {
            OriginalSchemaFields::Typed(fields) => fields.len(),
            OriginalSchemaFields::Inherited(fields) => fields.len(),
        };
        let mut loans = Vec::with_capacity(self.namespace.fields.len() + roots);
        loans.extend(self.namespace.fields.iter().cloned());
        // The original empty staging map remains allocation-free; Arrow 60
        // represents its attached empty Metadata as None, without an Arc.
        let metadata = MaterializedMetadataMap::new_for_original_reserve();
        // The two typed input containers share ONE original Field -> Arc ->
        // Schema conversion. Plain foreign fields retain no fabricated origin.
        fn publish(
            fields: impl Iterator<Item = OriginalFieldMaterialization>,
            roots: usize,
            loans: &mut Vec<MetadataFieldLoan>,
        ) -> Vec<FieldRef> {
            // Keep the real root buffer explicit: in-place Map collection from
            // a larger owned Field carrier could reuse its larger capacity.
            let mut published = Vec::with_capacity(roots);
            for field in fields {
                let (field, loan) = field.into_shared();
                if let Some(loan) = loan {
                    loans.push(loan);
                }
                published.push(field);
            }
            published
        }
        let fields = match self.fields {
            OriginalSchemaFields::Typed(fields) => publish(
                fields
                    .into_iter()
                    .map(OriginalFieldMaterialization::Materialized),
                roots,
                &mut loans,
            ),
            OriginalSchemaFields::Inherited(fields) => {
                publish(fields.into_iter(), roots, &mut loans)
            }
        };
        let schema = metadata.into_schema(fields).into_shared();
        SchemaMetadataMaterializations::from_materialized_owners(schema, loans.into())
    }
}

impl std::fmt::Display for MetadataMaterializationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ValueType(source) => source.fmt(f),
            source => write!(f, "metadata materialization refused: {source:?}"),
        }
    }
}
impl std::error::Error for MetadataMaterializationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::ValueType(source) => Some(source),
            Self::BTree(source) => Some(source),
            _ => None,
        }
    }
}
