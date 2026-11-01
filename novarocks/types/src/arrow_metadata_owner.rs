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

//! Immutable metadata owners with genuine fresh-construction receipts.
//!
//! These receipts describe allocation provenance only, never type/semantic
//! authority or a memory-accounting grant. There is deliberately no constructor
//! from arbitrary Metadata, and no mutation access after construction.
//! Source callers must cover the complete construction peak before building.

use std::collections::HashMap;
use std::mem::size_of;
use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef, Fields, Metadata, Schema, SchemaRef};
use novarocks_type_contract::owned_resources::arrow_metadata::hashmap_conversion_requests;
use novarocks_type_contract::owned_resources::hashmap::fresh_table_layout;

type Entry = (String, String);

#[derive(Clone, Copy, Debug)]
pub struct MetadataOwnerLimits {
    pub entries: usize,
    pub construction_bytes: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MetadataOwnerError {
    CapacityExceeded,
    AllocationFailed(novarocks_failure_contract::SharedFailureCause),
    DuplicateKey,
    MissingFieldOrigin,
}
impl MetadataOwnerError {
    fn allocation_failed(source: std::collections::TryReserveError) -> Self {
        Self::AllocationFailed(novarocks_failure_contract::SharedFailureCause::new(
            MetadataReservationFailure(source),
        ))
    }
}
#[derive(Debug)]
struct MetadataReservationFailure(std::collections::TryReserveError);
impl std::fmt::Display for MetadataReservationFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "metadata owner collection reservation failed: {}",
            self.0
        )
    }
}
impl std::error::Error for MetadataReservationFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}
impl novarocks_failure_contract::FailureProjection for MetadataReservationFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact, FailureReason, domain};
        FailureFact::new(
            FailureClass::Resource,
            FailureReason::new(domain::METADATA, std::num::NonZeroU16::new(2).unwrap()),
        )
    }
}

impl std::fmt::Display for MetadataOwnerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::CapacityExceeded => "metadata owner construction exceeds its capacity profile",
            Self::AllocationFailed(_) => "metadata owner collection reservation failed",
            Self::DuplicateKey => "metadata owner construction contains a duplicate key",
            Self::MissingFieldOrigin => "metadata origin is absent for the exact field identity",
        })
    }
}
impl std::error::Error for MetadataOwnerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::AllocationFailed(source) => Some(source),
            _ => None,
        }
    }
}
impl novarocks_failure_contract::FailureProjection for MetadataOwnerError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact, FailureReason, domain};
        let (class, code) = match self {
            Self::CapacityExceeded => (FailureClass::Resource, 1),
            Self::AllocationFailed(source) => return source.failure_fact(),
            Self::DuplicateKey => (FailureClass::Invariant, 3),
            Self::MissingFieldOrigin => (FailureClass::Invariant, 4),
        };
        FailureFact::new(
            class,
            FailureReason::new(domain::METADATA, std::num::NonZeroU16::new(code).unwrap()),
        )
    }
}

/// Owns freshly constructed immutable Arrow metadata. The original fallible
/// HashMap reservation and insertion author remains, then Arrow consumes it
/// into a BTree and Arc; no arbitrary source gains a receipt.
#[derive(Debug)]
pub struct ArrowMetadataOwner {
    metadata: Metadata,
    backing_bytes: usize,
    construction_bytes: usize,
}

impl ArrowMetadataOwner {
    /// Borrowed preflight over a Vec's initialized entries and actual spare
    /// capacity. No iteration of an unknown hash table and no allocation.
    pub fn preflight(
        entries: &Vec<Entry>,
        limits: MetadataOwnerLimits,
    ) -> Result<usize, MetadataOwnerError> {
        if entries.len() > limits.entries {
            return Err(MetadataOwnerError::CapacityExceeded);
        }
        let table = requested_metadata_bytes(entries.len())?;
        let vector = entries
            .capacity()
            .checked_mul(size_of::<Entry>())
            .ok_or(MetadataOwnerError::CapacityExceeded)?;
        let mut total = checked_add(table, vector)?;
        for (key, value) in entries {
            total = checked_add(total, key.capacity())?;
            total = checked_add(total, value.capacity())?;
            if total > limits.construction_bytes {
                return Err(MetadataOwnerError::CapacityExceeded);
            }
        }
        if total > limits.construction_bytes {
            return Err(MetadataOwnerError::CapacityExceeded);
        }
        Ok(total)
    }

    pub fn try_new(
        entries: Vec<Entry>,
        limits: MetadataOwnerLimits,
    ) -> Result<Self, MetadataOwnerError> {
        let construction_bytes = Self::preflight(&entries, limits)?;
        let mut values = HashMap::new();
        values
            .try_reserve(entries.len())
            .map_err(MetadataOwnerError::allocation_failed)?;
        for (key, value) in entries {
            if values.insert(key, value).is_some() {
                return Err(MetadataOwnerError::DuplicateKey);
            }
        }
        // Arrow 60 moves this exact original staging map through its own
        // collect/sort/BTree/Arc conversion. All temporary and retained requests
        // were covered before the original fallible reserve; no text is copied.
        let mut backing_bytes = hashmap_conversion_requests(values.len())
            .and_then(|requests| requests.retained_backing_upper_bound())
            .map_err(|_| MetadataOwnerError::CapacityExceeded)?;
        let metadata = Metadata::from(values);
        for (key, value) in &metadata {
            backing_bytes = checked_add(backing_bytes, key.capacity())?;
            backing_bytes = checked_add(backing_bytes, value.capacity())?;
        }
        if backing_bytes > construction_bytes {
            return Err(MetadataOwnerError::CapacityExceeded);
        }
        Ok(Self {
            metadata,
            backing_bytes,
            construction_bytes,
        })
    }

    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    /// Heap allocation upper bound only; the containing owner's inline size
    /// must be covered separately by the source/runtime scaffold proof.
    pub fn backing_bytes(&self) -> usize {
        self.backing_bytes
    }

    /// Heap peak of source Vec, moved String heaps and the original staging/conversion requests.
    /// Generating entries, inline owners and later Field/Schema/name/type
    /// allocations remain separate caller obligations.
    pub fn construction_bytes(&self) -> usize {
        self.construction_bytes
    }

    /// Attaches the exact map to a fresh Field and keeps it immutable through
    /// an Arc identity. This proves only this field's own metadata map;
    /// name/DataType/nested field allocations require their own proof.
    pub fn into_field(
        self,
        name: String,
        data_type: DataType,
        nullable: bool,
    ) -> MetadataOwnedField {
        self.into_field_definition(Field::new(name, data_type, nullable))
    }

    /// Moves this freshly constructed map into a field definition, preserving
    /// its physical and dictionary attributes. Any prior map on the definition
    /// is replaced and receives no provenance receipt.
    pub fn into_field_definition(self, definition: Field) -> MetadataOwnedField {
        MetadataOwnedField {
            field: Arc::new(definition.with_metadata(self.metadata)),
            metadata_backing_bytes: self.backing_bytes,
        }
    }

    /// Proves only the top-level schema metadata map. Incoming Fields and
    /// their nested allocations are separate owners/proofs.
    pub fn into_schema(self, fields: Fields) -> MetadataOwnedSchema {
        MetadataOwnedSchema {
            schema: Arc::new(Schema::new_with_metadata(fields, self.metadata)),
            metadata_backing_bytes: self.backing_bytes,
        }
    }
}

/// Arc clones retain the same immutable actual field owner. A structural Field
/// clone or Arc::make_mut copy is a different Field owner and cannot reuse this
/// proof, even when its immutable Metadata Arc remains shared.
#[derive(Clone, Debug)]
/// Proves only the attached metadata table and key/value String heaps,
/// excluding the Field/Arc/name/DataType/nested/wrapper scaffolds.
pub struct MetadataOwnedField {
    field: FieldRef,
    metadata_backing_bytes: usize,
}
impl MetadataOwnedField {
    pub fn field(&self) -> &FieldRef {
        &self.field
    }
    /// Derives only a metadata-map receipt from this known immutable source.
    /// Caller separately covers name/DataType clones and the new Field/Arc.
    pub fn derive_field(
        &self,
        data_type: DataType,
        nullable: bool,
        limits: MetadataOwnerLimits,
    ) -> Result<Self, MetadataOwnerError> {
        let metadata =
            clone_known_metadata(self.field.metadata(), self.metadata_backing_bytes, limits)?;
        // Arrow 60 Metadata::clone only shares the immutable map Arc. The
        // new Field/name/type scaffolds are separate caller requests. Only the deprecated
        // dictionary id is not carried over; NovaRocks never reads it.
        let field = Field::new(self.field.name().clone(), data_type, nullable)
            .with_dict_is_ordered(self.field.dict_is_ordered().unwrap_or(false))
            .with_metadata(metadata.metadata);
        Ok(Self {
            field: Arc::new(field),
            metadata_backing_bytes: metadata.backing_bytes,
        })
    }

    /// Metadata table + key/value heaps for this exact immutable field only.
    pub fn backing_bytes_for(&self, field: &FieldRef) -> Option<usize> {
        Arc::ptr_eq(&self.field, field).then_some(self.metadata_backing_bytes)
    }
}

/// Proves only the attached top-level metadata table and key/value String
/// heaps, excluding Schema/Arc/Fields/nested/wrapper scaffolds.
#[derive(Clone, Debug)]
pub struct MetadataOwnedSchema {
    schema: SchemaRef,
    metadata_backing_bytes: usize,
}
impl MetadataOwnedSchema {
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }
    /// The source map is known immutable. Field origins are supplied separately
    /// and are not inferred from the incoming Fields.
    pub fn derive_schema(
        &self,
        fields: Fields,
        limits: MetadataOwnerLimits,
    ) -> Result<Self, MetadataOwnerError> {
        let metadata =
            clone_known_metadata(self.schema.metadata(), self.metadata_backing_bytes, limits)?;
        Ok(metadata.into_schema(fields))
    }

    /// Top-level metadata table + key/value heaps for this exact schema only.
    pub fn backing_bytes_for(&self, schema: &SchemaRef) -> Option<usize> {
        Arc::ptr_eq(&self.schema, schema).then_some(self.metadata_backing_bytes)
    }
}

// Private: only exact immutable source-owner wrappers call this function.
// An arbitrary caller-owned Metadata can never obtain a receipt through it.
fn clone_known_metadata(
    source: &Metadata,
    source_backing_bytes: usize,
    limits: MetadataOwnerLimits,
) -> Result<ArrowMetadataOwner, MetadataOwnerError> {
    if source.len() > limits.entries || source_backing_bytes > limits.construction_bytes {
        return Err(MetadataOwnerError::CapacityExceeded);
    }
    // Arrow 60 clones its Option<Arc<BTreeMap>> without allocating, iterating
    // or copying text. The original backing remains live through both owners;
    // count it once in this construction peak and retain its original receipt.
    Ok(ArrowMetadataOwner {
        metadata: source.clone(),
        backing_bytes: source_backing_bytes,
        construction_bytes: source_backing_bytes,
    })
}

fn checked_add(left: usize, right: usize) -> Result<usize, MetadataOwnerError> {
    left.checked_add(right)
        .ok_or(MetadataOwnerError::CapacityExceeded)
}

fn requested_metadata_bytes(entries: usize) -> Result<usize, MetadataOwnerError> {
    // Keep the original fallible HashMap reservation as an independent source.
    // Arrow 60's conversion adds Vec/sort scratch, BTree nodes and its Arc.
    // Their cumulative source-derived quote also covers every coexistence peak.
    let staging = fresh_table_layout::<String, String>(entries)
        .map_err(|_| MetadataOwnerError::CapacityExceeded)?
        .request_bytes_upper_bound;
    let conversion = hashmap_conversion_requests(entries)
        .and_then(|requests| requests.request_bytes_upper_bound())
        .map_err(|_| MetadataOwnerError::CapacityExceeded)?;
    checked_add(staging, conversion)
}

/// A finite, immutable, sorted index of genuine attached-field metadata owners.
/// It proves only those metadata maps, never a DataType or whole schema. Lookup
/// checks exact Arc identity; no unknown table is iterated or normalized.
#[derive(Clone, Debug)]
pub struct FieldMetadataOrigins {
    owners: Arc<[MetadataOwnedField]>,
}
impl FieldMetadataOrigins {
    pub fn try_new(
        mut owners: Vec<MetadataOwnedField>,
        maximum_nodes: usize,
    ) -> Result<Self, MetadataOwnerError> {
        if owners.len() > maximum_nodes {
            return Err(MetadataOwnerError::CapacityExceeded);
        }
        owners.sort_unstable_by_key(|owner| Arc::as_ptr(owner.field()) as usize);
        owners.dedup_by(|a, b| Arc::ptr_eq(a.field(), b.field()));
        Ok(Self {
            owners: owners.into(),
        })
    }

    pub fn metadata_bytes_for(&self, field: &FieldRef) -> Option<usize> {
        let pointer = Arc::as_ptr(field) as usize;
        let index = self
            .owners
            .binary_search_by_key(&pointer, |owner| Arc::as_ptr(owner.field()) as usize)
            .ok()?;
        self.owners[index].backing_bytes_for(field)
    }

    pub fn owner_for(&self, field: &FieldRef) -> Option<&MetadataOwnedField> {
        let pointer = Arc::as_ptr(field) as usize;
        let index = self
            .owners
            .binary_search_by_key(&pointer, |owner| Arc::as_ptr(owner.field()) as usize)
            .ok()?;
        Arc::ptr_eq(self.owners[index].field(), field).then_some(&self.owners[index])
    }

    /// New root-field properties keep existing child metadata origins. Newly
    /// constructed child fields still require their own genuine receipts.
    pub fn replacing_root(
        &self,
        old: &FieldRef,
        replacement: MetadataOwnedField,
        maximum_nodes: usize,
    ) -> Result<Self, MetadataOwnerError> {
        let mut owners = Vec::with_capacity(self.owners.len());
        for owner in self.owners.iter() {
            if !Arc::ptr_eq(owner.field(), old) {
                owners.push(owner.clone());
            }
        }
        owners.push(replacement);
        Self::try_new(owners, maximum_nodes)
    }

    /// Retain only exact metadata owners reachable through this field's type
    /// tree. This avoids one whole-layout provenance index per projected slot.
    pub fn for_field_tree(
        &self,
        root: &FieldRef,
        maximum_nodes: usize,
        maximum_depth: usize,
    ) -> Result<Self, MetadataOwnerError> {
        let mut owners = Vec::new();
        self.collect_field(root, 0, maximum_nodes, maximum_depth, &mut owners, &mut 0)?;
        Self::try_new(owners, maximum_nodes)
    }

    fn collect_field(
        &self,
        field: &FieldRef,
        depth: usize,
        nodes: usize,
        max_depth: usize,
        owners: &mut Vec<MetadataOwnedField>,
        visited_types: &mut usize,
    ) -> Result<(), MetadataOwnerError> {
        if depth > max_depth || owners.len() >= nodes {
            return Err(MetadataOwnerError::CapacityExceeded);
        }
        let owner = self
            .owner_for(field)
            .ok_or(MetadataOwnerError::MissingFieldOrigin)?;
        owners.push(owner.clone());
        self.collect_type(
            field.data_type(),
            depth,
            nodes,
            max_depth,
            owners,
            visited_types,
        )
    }

    fn collect_type(
        &self,
        data_type: &DataType,
        depth: usize,
        nodes: usize,
        max_depth: usize,
        owners: &mut Vec<MetadataOwnedField>,
        visited_types: &mut usize,
    ) -> Result<(), MetadataOwnerError> {
        *visited_types = visited_types
            .checked_add(1)
            .filter(|count| *count <= nodes)
            .ok_or(MetadataOwnerError::CapacityExceeded)?;
        // Dictionary key/value scalar leaves are physical storage at their
        // parent's semantic level. Composite recursion still consumes depth.
        if depth > max_depth
            && matches!(
                data_type,
                DataType::List(_)
                    | DataType::LargeList(_)
                    | DataType::ListView(_)
                    | DataType::LargeListView(_)
                    | DataType::FixedSizeList(_, _)
                    | DataType::Map(_, _)
                    | DataType::Struct(_)
                    | DataType::Union(_, _)
                    | DataType::Dictionary(_, _)
                    | DataType::RunEndEncoded(_, _)
            )
        {
            return Err(MetadataOwnerError::CapacityExceeded);
        }
        match data_type {
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::ListView(field)
            | DataType::LargeListView(field)
            | DataType::FixedSizeList(field, _) => {
                self.collect_field(field, depth + 1, nodes, max_depth, owners, visited_types)?;
            }
            DataType::Map(entries, _) => {
                // The physical entries Struct is part of this semantic level.
                self.collect_field(entries, depth, nodes, max_depth, owners, visited_types)?;
            }
            DataType::Struct(fields) => {
                for field in fields {
                    self.collect_field(field, depth + 1, nodes, max_depth, owners, visited_types)?;
                }
            }
            DataType::Union(fields, _) => {
                for (_, field) in fields.iter() {
                    self.collect_field(field, depth + 1, nodes, max_depth, owners, visited_types)?;
                }
            }
            DataType::Dictionary(key, value) => {
                if depth >= max_depth
                    && (matches!(key.as_ref(), DataType::Dictionary(_, _))
                        || matches!(value.as_ref(), DataType::Dictionary(_, _)))
                {
                    return Err(MetadataOwnerError::CapacityExceeded);
                }
                self.collect_type(key, depth + 1, nodes, max_depth, owners, visited_types)?;
                self.collect_type(value, depth + 1, nodes, max_depth, owners, visited_types)?;
            }
            DataType::RunEndEncoded(runs, values) => {
                self.collect_field(runs, depth + 1, nodes, max_depth, owners, visited_types)?;
                self.collect_field(values, depth + 1, nodes, max_depth, owners, visited_types)?;
            }
            _ => {}
        }
        Ok(())
    }

    pub fn owners(&self) -> &[MetadataOwnedField] {
        &self.owners
    }

    /// Only the index's own compact Arc allocation, excluding pointed-to
    /// fields/maps and this wrapper's inline size.
    pub fn index_backing_bytes(&self) -> usize {
        self.owners.len() * size_of::<MetadataOwnedField>() + 4 * size_of::<usize>()
    }
}

#[cfg(test)]
mod failure_tests {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    #[test]
    fn actual_standard_reservation_source_survives_clone_without_inferred_quota() {
        use std::error::Error;
        let source = Vec::<u8>::new().try_reserve(usize::MAX).unwrap_err();
        let failure = MetadataOwnerError::allocation_failed(source);
        let copied = failure.clone();
        assert_eq!(failure.failure_fact().class(), FailureClass::Resource);
        assert_eq!(failure.failure_fact().reason().code(), 2);
        assert!(failure.failure_fact().resource().is_none());
        let source = failure.source().unwrap().source().unwrap();
        let copied_source = copied.source().unwrap().source().unwrap();
        assert!(std::ptr::eq(source, copied_source));
        assert!(
            source
                .source()
                .unwrap()
                .is::<std::collections::TryReserveError>()
        );
    }
    #[test]
    fn actual_capacity_and_duplicate_key_refusals_project_distinct_original_facts() {
        let limits = MetadataOwnerLimits {
            entries: 0,
            construction_bytes: usize::MAX,
        };
        let capacity =
            ArrowMetadataOwner::try_new(vec![("key".into(), "value".into())], limits).unwrap_err();
        assert_eq!(capacity, MetadataOwnerError::CapacityExceeded);
        assert_eq!(capacity.failure_fact().class(), FailureClass::Resource);
        assert_eq!(capacity.failure_fact().reason().domain(), 8);
        assert_eq!(capacity.failure_fact().reason().code(), 1);
        let duplicate = ArrowMetadataOwner::try_new(
            vec![("key".into(), "one".into()), ("key".into(), "two".into())],
            MetadataOwnerLimits {
                entries: 2,
                construction_bytes: usize::MAX,
            },
        )
        .unwrap_err();
        assert_eq!(duplicate, MetadataOwnerError::DuplicateKey);
        assert_eq!(duplicate.failure_fact().class(), FailureClass::Invariant);
        assert_eq!(duplicate.failure_fact().reason().code(), 3);
        assert_eq!(capacity.failure_fact().resource(), None);
    }
}
