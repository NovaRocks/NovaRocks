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
//! Borrowed inspection of actual Arrow schema/type heap backing.
//!
//! This is only the schema/type portion of a root input proof. Standard array
//! storage, Chunk scaffolds and the caller's pre-allocation funding remain
//! separate obligations. Metadata is accepted only through immutable exact-Arc
//! construction receipts. No unknown metadata map is inspected or copied.

use std::alloc::Layout;
use std::sync::atomic::AtomicUsize;

use arrow::datatypes::{DataType, FieldRef, Fields, SchemaRef, TimeUnit};
use novarocks_result_contract::RootProfileV1;
use novarocks_type_contract::owned_resources::metadata_materialization::{
    MetadataFieldLoan, MetadataMaterializationError, SharedMaterializedSchema,
};
use novarocks_types::arrow_metadata_owner::{FieldMetadataOrigins, MetadataOwnedSchema};

/// Borrowed, positively authored namespaces. No content equality or numerical
/// conversion between independent construction receipts establishes identity.
#[derive(Clone, Copy)]
pub(crate) enum RootFieldSource<'a> {
    /// Only the private immutable typed ChunkSchema can elect this source.
    /// No retained metadata table exists; each actual Field is still checked.
    CanonicalExecution,
    M07(&'a FieldMetadataOrigins),
    Original {
        index: &'a [MetadataFieldLoan],
        attached: Option<&'a FieldMetadataOrigins>,
    },
}
impl RootFieldSource<'_> {
    fn metadata_bytes_for(
        self,
        field: &FieldRef,
        observe: &mut impl FnMut() -> Result<(), RootSchemaBackingError>,
    ) -> Result<usize, RootSchemaBackingError> {
        match self {
            Self::CanonicalExecution if field.metadata().as_arc().is_none() => Ok(0),
            Self::CanonicalExecution => Err(RootSchemaBackingError::UnknownFieldMetadataOwner),
            Self::M07(origins) => origins
                .metadata_bytes_for(field)
                .ok_or(RootSchemaBackingError::UnknownFieldMetadataOwner),
            Self::Original { index, attached } => {
                let pointer = std::sync::Arc::as_ptr(field) as usize;
                if let Ok(at) =
                    index.binary_search_by_key(&pointer, MetadataFieldLoan::original_address)
                {
                    return index[at]
                        .original_backing_observed(field, observe)
                        .ok_or(RootSchemaBackingError::UnknownFieldMetadataOwner)?;
                }
                // This is a second explicitly attached positive namespace, not
                // a reconstruction from equal contents or an unknown table.
                attached
                    .and_then(|origins| origins.metadata_bytes_for(field))
                    .ok_or(RootSchemaBackingError::UnknownFieldMetadataOwner)
            }
        }
    }
    fn same_index(self, other: Self) -> bool {
        match (self, other) {
            (Self::CanonicalExecution, Self::CanonicalExecution) => true,
            (Self::M07(a), Self::M07(b)) => {
                a.owners().as_ptr() == b.owners().as_ptr() && a.owners().len() == b.owners().len()
            }
            (
                Self::Original {
                    index: a,
                    attached: x,
                },
                Self::Original {
                    index: b,
                    attached: y,
                },
            ) => {
                a.as_ptr() == b.as_ptr()
                    && a.len() == b.len()
                    && match (x, y) {
                        (None, None) => true,
                        (Some(x), Some(y)) => {
                            x.owners().as_ptr() == y.owners().as_ptr()
                                && x.owners().len() == y.owners().len()
                        }
                        _ => false,
                    }
            }
            _ => false,
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum RootSchemaSource<'a> {
    CanonicalExecution,
    M07(&'a MetadataOwnedSchema),
    Original(&'a SharedMaterializedSchema),
}

impl From<MetadataMaterializationError> for RootSchemaBackingError {
    fn from(error: MetadataMaterializationError) -> Self {
        match error {
            MetadataMaterializationError::Arithmetic
            | MetadataMaterializationError::BTree(
                novarocks_type_contract::owned_resources::btree::BTreeResourceError::Arithmetic(_),
            ) => Self::CapacityExceeded,
            // This boundary deliberately has allocation-free coarse receipts;
            // detailed BTree sources remain on upstream numerical envelopes.
            MetadataMaterializationError::BTree(
                novarocks_type_contract::owned_resources::btree::BTreeResourceError::SourceModel(_),
            ) => Self::UnknownFieldMetadataOwner,
            _ => Self::UnknownFieldMetadataOwner,
        }
    }
}

const MAX_BYTES: usize = 96 * 1024 * 1024;
const MAX_AUXILIARY_NODES: usize = 65_536;

/// Inspection and refusal paths need no allocated diagnostic.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RootSchemaBackingError {
    UnknownSchemaMetadataOwner,
    UnknownFieldMetadataOwner,
    UninspectedOriginIndex,
    UnsupportedType,
    CapacityExceeded,
    WorkExceeded,
}

/// The exact header layout used by pinned Rust 1.98.1's ArcInner<T>:
/// alloc/src/sync.rs, repr(C, align(2)), strong + weak + data. Layout extension
/// also covers tail alignment and final padding, rather than guessing a size.
#[repr(C, align(2))]
struct ArcHeader {
    strong: AtomicUsize,
    weak: AtomicUsize,
}

/// One cumulative byte budget, a bounded carrier type tree walk, and bounded
/// auxiliary work for compact provenance indices and actual array DataTypes.
/// Aliases are charged in full every time; there is no allocation identity map.
/// Any inspection error rejects the input; a partial walk is never a proof.
pub(crate) struct RootSchemaInspection<'a> {
    byte_limit: usize,
    node_limit: usize,
    depth_limit: usize,
    bytes: usize,
    nodes: usize,
    auxiliary_nodes: usize,
    // Keep the checked immutable backing alive by borrowing its owner. A raw
    // cached pointer alone could accept a later allocator address reuse.
    inspected_origin_index: Option<RootFieldSource<'a>>,
}

impl<'a> RootSchemaInspection<'a> {
    pub(crate) fn new(bytes: usize, nodes: usize, depth: usize) -> Self {
        Self {
            byte_limit: bytes.min(MAX_BYTES),
            // Carrier-only Dictionary key/value and Map entries also consume
            // work. This does not enlarge the frozen semantic schema budget.
            node_limit: nodes.min(2 * RootProfileV1::SCHEMA_TYPE_NODES),
            depth_limit: depth.min(RootProfileV1::MAX_DEPTH),
            bytes: 0,
            nodes: 0,
            auxiliary_nodes: 0,
            inspected_origin_index: None,
        }
    }

    pub(crate) fn bytes(&self) -> usize {
        self.bytes
    }

    /// Allows the caller to include its own precisely known scaffolds under
    /// the same budget. It grants no tracker/funding authority.
    pub(crate) fn charge(&mut self, bytes: usize) -> Result<(), RootSchemaBackingError> {
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .filter(|total| *total <= self.byte_limit)
            .ok_or(RootSchemaBackingError::CapacityExceeded)?;
        Ok(())
    }

    /// Full requested Arc allocation for a sized or unsized borrowed payload.
    /// The caller must know this payload is held in an ordinary std Arc; this
    /// does not establish allocation ownership for an arbitrary reference.
    pub(crate) fn charge_arc<T: ?Sized>(
        &mut self,
        payload: &T,
    ) -> Result<(), RootSchemaBackingError> {
        self.charge_arc_layout(Layout::for_value(payload))
    }

    pub(crate) fn enter_auxiliary_node(
        &mut self,
        depth: usize,
    ) -> Result<(), RootSchemaBackingError> {
        if depth > self.depth_limit {
            return Err(RootSchemaBackingError::WorkExceeded);
        }
        self.auxiliary_nodes = self
            .auxiliary_nodes
            .checked_add(1)
            .filter(|nodes| *nodes <= MAX_AUXILIARY_NODES)
            .ok_or(RootSchemaBackingError::WorkExceeded)?;
        Ok(())
    }

    /// Check every retained owner, including owners unreachable from the
    /// current schema. Each owner charges its own Field/name/map and DataType
    /// heaps. Child Field owners are checked for membership, not recursively
    /// walked here, so a deeply nested index takes bounded linear work.
    ///
    /// On success this exact immutable index can support inspect_data_type.
    /// If several per-slot indices are inspected, inspect the aggregate index
    /// last before inspecting actual array DataTypes.
    pub(crate) fn inspect_origin_index(
        &mut self,
        origins: &'a FieldMetadataOrigins,
    ) -> Result<(), RootSchemaBackingError> {
        self.inspected_origin_index = None;
        let owners = origins.owners();
        self.preflight_auxiliary(owners.len())?;
        self.charge_arc(owners)?;
        for owner in owners {
            self.enter_auxiliary_node(0)?;
            self.field_own_heap(owner.field(), RootFieldSource::M07(origins))?;
            self.type_own_heap(
                owner.field().data_type(),
                RootFieldSource::M07(origins),
                0,
                false,
            )?;
        }
        self.inspected_origin_index = Some(RootFieldSource::M07(origins));
        Ok(())
    }

    /// The actual schema tree is independently checked for bounded semantic
    /// depth and carrier node count. Exact field receipts come from the supplied index;
    /// structural equality with a compiled schema is never sufficient.
    #[cfg(test)]
    pub(crate) fn inspect_schema(
        &mut self,
        schema: &SchemaRef,
        owner: &MetadataOwnedSchema,
        origins: &FieldMetadataOrigins,
    ) -> Result<(), RootSchemaBackingError> {
        self.inspect_schema_source(
            schema,
            RootSchemaSource::M07(owner),
            RootFieldSource::M07(origins),
        )
    }

    pub(crate) fn inspect_original_index(
        &mut self,
        index: &'a [MetadataFieldLoan],
        attached: Option<&'a FieldMetadataOrigins>,
    ) -> Result<(), RootSchemaBackingError> {
        self.inspected_origin_index = None;
        self.preflight_auxiliary(index.len())?;
        if !index.is_sorted_by_key(MetadataFieldLoan::original_address) {
            return Err(RootSchemaBackingError::UninspectedOriginIndex);
        }
        self.charge_arc(index)?;
        let source = RootFieldSource::Original { index, attached };
        for loan in index {
            self.enter_auxiliary_node(0)?;
            if let Some(field) = loan.borrow_original_field() {
                self.field_own_heap(&field, source)?;
                self.type_own_heap(field.data_type(), source, 0, false)?;
            } else {
                // Weak pins the complete ArcInner allocation after payload
                // destruction, but no destroyed String/map/type heap remains.
                self.charge_arc_layout(Layout::new::<arrow::datatypes::Field>())?;
            }
        }
        self.inspected_origin_index = Some(source);
        Ok(())
    }

    pub(crate) fn inspect_schema_source(
        &mut self,
        schema: &SchemaRef,
        owner: RootSchemaSource<'_>,
        origins: RootFieldSource<'_>,
    ) -> Result<(), RootSchemaBackingError> {
        let metadata = match owner {
            RootSchemaSource::CanonicalExecution if schema.metadata().as_arc().is_none() => 0,
            RootSchemaSource::CanonicalExecution => {
                return Err(RootSchemaBackingError::UnknownSchemaMetadataOwner);
            }
            RootSchemaSource::M07(owner) => owner
                .backing_bytes_for(schema)
                .ok_or(RootSchemaBackingError::UnknownSchemaMetadataOwner)?,
            RootSchemaSource::Original(owner) => owner
                .original_backing_observed(schema, &mut || self.enter_auxiliary_node(0))
                .ok_or(RootSchemaBackingError::UnknownSchemaMetadataOwner)??,
        };
        if schema.fields.len() > RootProfileV1::MAX_COLUMNS {
            return Err(RootSchemaBackingError::WorkExceeded);
        }
        self.preflight_tree_fields(&schema.fields)?;
        self.charge_arc(schema.as_ref())?;
        self.charge(metadata)?;
        self.charge_arc(&schema.fields[..])?;
        for field in &schema.fields {
            self.inspect_field_at(field, origins, 0)?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn inspect_field(
        &mut self,
        field: &FieldRef,
        origins: &FieldMetadataOrigins,
    ) -> Result<(), RootSchemaBackingError> {
        self.inspect_field_at(field, RootFieldSource::M07(origins), 0)
    }

    /// Inspect only the borrowed DataType's own variable heaps and direct
    /// child-field membership, after closed standard carrier validation.
    /// Every child Field/name/map/type heap is already covered by the complete
    /// immutable index inspection. Rewalking child trees for each array would
    /// turn a valid deeply nested carrier into quadratic work.
    #[cfg(test)]
    pub(crate) fn inspect_data_type(
        &mut self,
        data_type: &DataType,
        origins: &FieldMetadataOrigins,
    ) -> Result<(), RootSchemaBackingError> {
        self.inspect_data_type_source(data_type, RootFieldSource::M07(origins))
    }

    pub(crate) fn inspect_data_type_source(
        &mut self,
        data_type: &DataType,
        origins: RootFieldSource<'_>,
    ) -> Result<(), RootSchemaBackingError> {
        if !matches!(origins, RootFieldSource::CanonicalExecution)
            && !self
                .inspected_origin_index
                .is_some_and(|checked| checked.same_index(origins))
        {
            return Err(RootSchemaBackingError::UninspectedOriginIndex);
        }
        self.type_own_heap(data_type, origins, 0, true)
    }

    fn charge_arc_layout(&mut self, payload: Layout) -> Result<(), RootSchemaBackingError> {
        let layout = Layout::new::<ArcHeader>()
            .extend(payload)
            .map_err(|_| RootSchemaBackingError::CapacityExceeded)?
            .0
            .pad_to_align();
        self.charge(layout.size())
    }

    fn field_own_heap(
        &mut self,
        field: &FieldRef,
        origins: RootFieldSource<'_>,
    ) -> Result<(), RootSchemaBackingError> {
        let metadata = origins.metadata_bytes_for(field, &mut || self.enter_auxiliary_node(0))?;
        self.charge_arc(field.as_ref())?;
        self.charge(field.name().capacity())?;
        self.charge(metadata)
    }

    fn inspect_field_at(
        &mut self,
        field: &FieldRef,
        origins: RootFieldSource<'_>,
        depth: usize,
    ) -> Result<(), RootSchemaBackingError> {
        self.enter_tree_node(depth)?;
        self.field_own_heap(field, origins)?;
        let data_type = field.data_type();
        self.type_own_heap(data_type, origins, depth, false)?;
        match data_type {
            DataType::List(child) | DataType::LargeList(child) => {
                self.inspect_field_at(child, origins, depth + 1)?;
            }
            DataType::Map(entries, _) => {
                // The physical entries Struct belongs to the Map's semantic
                // level; its key/value fields consume the one nested level.
                self.inspect_field_at(entries, origins, depth)?;
            }
            DataType::Struct(fields) => {
                self.preflight_tree_fields(fields)?;
                for child in fields {
                    self.inspect_field_at(child, origins, depth + 1)?;
                }
            }
            DataType::Dictionary(_, _) => {
                // The closed dictionary vocabulary below has two primitive
                // boxed DataTypes. Count their work without a nesting level.
                self.enter_tree_node(depth)?;
                self.enter_tree_node(depth)?;
            }
            _ => {}
        }
        Ok(())
    }

    /// Own variable heap only: DataType is inline in its containing Field or
    /// standard array. Fields is a complete Arc slice, with no growable spare
    /// capacity. Dictionary key/value are independent Box allocations.
    fn type_own_heap(
        &mut self,
        data_type: &DataType,
        origins: RootFieldSource<'_>,
        depth: usize,
        actual_array_type: bool,
    ) -> Result<(), RootSchemaBackingError> {
        self.enter_auxiliary_node(depth)?;
        match data_type {
            DataType::Null
            | DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(TimeUnit::Second | TimeUnit::Millisecond)
            | DataType::Time64(TimeUnit::Microsecond | TimeUnit::Nanosecond)
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::FixedSizeBinary(_) => {}
            DataType::Timestamp(_, zone) => {
                if let Some(zone) = zone {
                    self.charge_arc(zone.as_ref())?;
                }
            }
            DataType::List(child) | DataType::LargeList(child) | DataType::Map(child, _) => {
                self.require_child(child, origins, depth, actual_array_type)?;
            }
            DataType::Struct(fields) => {
                self.preflight_auxiliary(fields.len())?;
                self.charge_arc(&fields[..])?;
                for child in fields {
                    self.require_child(child, origins, depth, actual_array_type)?;
                }
            }
            DataType::Dictionary(key, value)
                if matches!(key.as_ref(), DataType::Int32)
                    && matches!(value.as_ref(), DataType::Utf8 | DataType::LargeUtf8) =>
            {
                self.charge(Layout::new::<DataType>().size())?;
                self.charge(Layout::new::<DataType>().size())?;
                // The guarded key/value variants have no own variable heap.
                // Their constant checks need no repeated auxiliary traversal;
                // the tree walk separately counts both physical type nodes.
            }
            _ => return Err(RootSchemaBackingError::UnsupportedType),
        }
        Ok(())
    }

    fn require_child(
        &mut self,
        child: &FieldRef,
        origins: RootFieldSource<'_>,
        depth: usize,
        actual_array_type: bool,
    ) -> Result<(), RootSchemaBackingError> {
        self.enter_auxiliary_node(depth)?;
        // Membership does not iterate or format unknown metadata. Exact
        // source lookup alone proves the child's independent construction.
        match origins {
            RootFieldSource::CanonicalExecution => {
                if child.metadata().as_arc().is_some() {
                    return Err(RootSchemaBackingError::UnknownFieldMetadataOwner);
                }
                if actual_array_type {
                    // No closed origin index covers an actual array's child
                    // Field. Equal fields may have independent name backing.
                    // Charge only this direct Arc/name; that child's own array
                    // callback covers its DataType heaps, without a subtree
                    // walk per parent (which would make nesting quadratic).
                    self.field_own_heap(child, origins)?;
                }
            }
            RootFieldSource::M07(origins) => {
                if origins.metadata_bytes_for(child).is_none() {
                    return Err(RootSchemaBackingError::UnknownFieldMetadataOwner);
                }
            }
            RootFieldSource::Original { index, attached } => {
                let pointer = std::sync::Arc::as_ptr(child) as usize;
                if index
                    .binary_search_by_key(&pointer, MetadataFieldLoan::original_address)
                    .is_err()
                    && attached.and_then(|v| v.metadata_bytes_for(child)).is_none()
                {
                    return Err(RootSchemaBackingError::UnknownFieldMetadataOwner);
                }
            }
        }
        Ok(())
    }

    fn enter_tree_node(&mut self, depth: usize) -> Result<(), RootSchemaBackingError> {
        if depth > self.depth_limit {
            return Err(RootSchemaBackingError::WorkExceeded);
        }
        self.nodes = self
            .nodes
            .checked_add(1)
            .filter(|nodes| *nodes <= self.node_limit)
            .ok_or(RootSchemaBackingError::WorkExceeded)?;
        Ok(())
    }

    fn preflight_tree_fields(&self, fields: &Fields) -> Result<(), RootSchemaBackingError> {
        if fields.len() > self.node_limit.saturating_sub(self.nodes) {
            return Err(RootSchemaBackingError::WorkExceeded);
        }
        Ok(())
    }

    fn preflight_auxiliary(&self, nodes: usize) -> Result<(), RootSchemaBackingError> {
        if nodes > MAX_AUXILIARY_NODES.saturating_sub(self.auxiliary_nodes) {
            return Err(RootSchemaBackingError::WorkExceeded);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::datatypes::{Field, Schema};
    use novarocks_types::arrow_metadata_owner::{
        ArrowMetadataOwner, MetadataOwnedField, MetadataOwnerLimits,
    };

    fn metadata(entries: Vec<(String, String)>) -> ArrowMetadataOwner {
        ArrowMetadataOwner::try_new(
            entries,
            MetadataOwnerLimits {
                entries: 4096,
                construction_bytes: MAX_BYTES,
            },
        )
        .unwrap()
    }

    fn field(name: String, data_type: DataType) -> MetadataOwnedField {
        metadata(vec![]).into_field(name, data_type, true)
    }

    fn inspection<'a>() -> RootSchemaInspection<'a> {
        RootSchemaInspection::new(MAX_BYTES, 2 * RootProfileV1::SCHEMA_TYPE_NODES, 64)
    }

    fn arc_bytes<T: ?Sized>(value: &T) -> usize {
        Layout::new::<ArcHeader>()
            .extend(Layout::for_value(value))
            .unwrap()
            .0
            .pad_to_align()
            .size()
    }

    #[test]
    fn full_name_capacity_exact_metadata_and_arc_slice_are_charged() {
        let mut name = String::with_capacity(8192);
        name.push('x');
        let leaf =
            metadata(vec![("tag".into(), "json".into())]).into_field(name, DataType::Utf8, true);
        let parent = field("list".into(), DataType::List(leaf.field().clone()));
        let origins = FieldMetadataOrigins::try_new(vec![leaf.clone(), parent.clone()], 8).unwrap();
        let schema = metadata(vec![("schema".into(), "source".into())])
            .into_schema(vec![parent.field().clone()].into());
        let mut state = inspection();
        state
            .inspect_schema(schema.schema(), &schema, &origins)
            .unwrap();
        let expected = arc_bytes(schema.schema().as_ref())
            + schema.backing_bytes_for(schema.schema()).unwrap()
            + arc_bytes(&schema.schema().fields[..])
            + arc_bytes(parent.field().as_ref())
            + parent.field().name().capacity()
            + parent.backing_bytes_for(parent.field()).unwrap()
            + arc_bytes(leaf.field().as_ref())
            + leaf.field().name().capacity()
            + leaf.backing_bytes_for(leaf.field()).unwrap();
        assert_eq!(state.bytes(), expected);
        assert_eq!(state.nodes, 2);
        assert!(expected >= 8192);
        let mut tight = RootSchemaInspection::new(expected - 1, 8, 64);
        assert_eq!(
            tight.inspect_schema(schema.schema(), &schema, &origins),
            Err(RootSchemaBackingError::CapacityExceeded)
        );
    }

    #[test]
    fn equal_unknown_field_and_schema_never_reuse_origin() {
        let known = field("x".into(), DataType::Int32);
        let origins = FieldMetadataOrigins::try_new(vec![known.clone()], 1).unwrap();
        let unknown = Arc::new(known.field().as_ref().clone());
        assert_eq!(
            inspection().inspect_field(&unknown, &origins),
            Err(RootSchemaBackingError::UnknownFieldMetadataOwner)
        );
        let schema = metadata(vec![]).into_schema(vec![known.field().clone()].into());
        let unknown_schema = Arc::new(schema.schema().as_ref().clone());
        assert_eq!(
            inspection().inspect_schema(&unknown_schema, &schema, &origins),
            Err(RootSchemaBackingError::UnknownSchemaMetadataOwner)
        );
        // Even an empty unknown map cannot be accepted by inspecting its len.
        let empty_unknown = Arc::new(Field::new("x", DataType::Int32, true));
        assert_eq!(
            inspection().inspect_field(&empty_unknown, &origins),
            Err(RootSchemaBackingError::UnknownFieldMetadataOwner)
        );
    }

    #[test]
    fn origin_index_covers_unreachable_owner_and_requires_complete_closure() {
        let known = field("x".into(), DataType::Int32);
        let unreachable = metadata(vec![("large".into(), "a".repeat(8192))]).into_field(
            "cached".into(),
            DataType::Utf8,
            true,
        );
        let origins =
            FieldMetadataOrigins::try_new(vec![known.clone(), unreachable.clone()], 2).unwrap();
        let mut state = inspection();
        state.inspect_origin_index(&origins).unwrap();
        let expected = arc_bytes(origins.owners())
            + arc_bytes(known.field().as_ref())
            + known.field().name().capacity()
            + known.backing_bytes_for(known.field()).unwrap()
            + arc_bytes(unreachable.field().as_ref())
            + unreachable.field().name().capacity()
            + unreachable.backing_bytes_for(unreachable.field()).unwrap();
        assert_eq!(state.bytes(), expected);
        let missing_child = field("list".into(), DataType::List(known.field().clone()));
        let incomplete = FieldMetadataOrigins::try_new(vec![missing_child], 1).unwrap();
        assert_eq!(
            inspection().inspect_origin_index(&incomplete),
            Err(RootSchemaBackingError::UnknownFieldMetadataOwner)
        );
    }

    #[test]
    fn canonical_actual_type_counts_independent_child_name_backing_shallowly() {
        let zone: Arc<str> = Arc::from("z".repeat(8192));
        let mut name = String::with_capacity(16384);
        name.push_str("item");
        let child = Arc::new(Field::new(
            name,
            DataType::Timestamp(TimeUnit::Microsecond, Some(zone.clone())),
            true,
        ));
        let actual = DataType::List(child.clone());
        let mut state = inspection();
        state
            .inspect_data_type_source(&actual, RootFieldSource::CanonicalExecution)
            .unwrap();
        // A shallow parent callback covers its actual Field/name, even though
        // an equal frozen layout could own a different small String/Field.
        assert_eq!(
            state.bytes(),
            arc_bytes(child.as_ref()) + child.name().capacity()
        );
        let before = state.bytes();
        state
            .inspect_data_type_source(child.data_type(), RootFieldSource::CanonicalExecution)
            .unwrap();
        assert_eq!(state.bytes() - before, arc_bytes(zone.as_ref()));
        let mut small = RootSchemaInspection::new(1024, 8192, 64);
        assert_eq!(
            small.inspect_data_type_source(&actual, RootFieldSource::CanonicalExecution),
            Err(RootSchemaBackingError::CapacityExceeded)
        );
    }

    #[test]
    fn canonical_schema_and_actual_child_accept_cleared_staging_and_reject_owned_metadata() {
        let mut spare = HashMap::with_capacity(256);
        spare.insert("discarded".to_owned(), "source attribute".to_owned());
        spare.clear();
        assert!(spare.is_empty() && spare.capacity() > 0);
        let mut child_spare = HashMap::with_capacity(256);
        child_spare.insert("discarded".to_owned(), "source attribute".to_owned());
        child_spare.clear();
        assert!(child_spare.is_empty() && child_spare.capacity() > 0);
        let child = Arc::new(Field::new("item", DataType::Int32, true).with_metadata(child_spare));
        assert!(child.metadata().as_arc().is_none());
        let mut state = inspection();
        state
            .inspect_data_type_source(&DataType::List(child), RootFieldSource::CanonicalExecution)
            .unwrap();
        let child = Arc::new(Field::new("item", DataType::Int32, true).with_metadata(
            HashMap::from([("source".to_owned(), "attribute".to_owned())]),
        ));
        assert!(child.metadata().as_arc().is_some());
        assert_eq!(
            state.inspect_data_type_source(
                &DataType::List(child),
                RootFieldSource::CanonicalExecution
            ),
            Err(RootSchemaBackingError::UnknownFieldMetadataOwner)
        );
        let schema = Arc::new(
            Schema::new(vec![Field::new("value", DataType::Int32, true)]).with_metadata(spare),
        );
        assert!(schema.metadata().as_arc().is_none());
        state
            .inspect_schema_source(
                &schema,
                RootSchemaSource::CanonicalExecution,
                RootFieldSource::CanonicalExecution,
            )
            .unwrap();
        let schema = Arc::new(schema.as_ref().clone().with_metadata(HashMap::from([(
            "source".to_owned(),
            "attribute".to_owned(),
        )])));
        assert!(schema.metadata().as_arc().is_some());
        assert_eq!(
            state.inspect_schema_source(
                &schema,
                RootSchemaSource::CanonicalExecution,
                RootFieldSource::CanonicalExecution
            ),
            Err(RootSchemaBackingError::UnknownSchemaMetadataOwner)
        );
    }

    #[test]
    fn actual_type_extras_require_the_prechecked_exact_index() {
        let leaf = field("x".into(), DataType::Utf8);
        let origins = FieldMetadataOrigins::try_new(vec![leaf.clone()], 1).unwrap();
        let other = FieldMetadataOrigins::try_new(vec![leaf.clone()], 1).unwrap();
        let actual = DataType::Struct(vec![leaf.field().clone()].into());
        let mut state = inspection();
        assert_eq!(
            state.inspect_data_type(&actual, &origins),
            Err(RootSchemaBackingError::UninspectedOriginIndex)
        );
        state.inspect_origin_index(&origins).unwrap();
        let before = state.bytes();
        state.inspect_data_type(&actual, &origins).unwrap();
        let DataType::Struct(fields) = &actual else {
            unreachable!()
        };
        assert_eq!(state.bytes() - before, arc_bytes(&fields[..]));
        assert_eq!(
            state.inspect_data_type(&actual, &other),
            Err(RootSchemaBackingError::UninspectedOriginIndex)
        );
        // Arc clones of the compact index are the same backing proof.
        state.inspect_data_type(&actual, &origins.clone()).unwrap();
    }

    #[test]
    fn dictionary_boxes_and_timezone_full_arc_tail_are_charged() {
        let origins = FieldMetadataOrigins::try_new(vec![], 0).unwrap();
        let mut state = inspection();
        state.inspect_origin_index(&origins).unwrap();
        let before = state.bytes();
        state
            .inspect_data_type(
                &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                &origins,
            )
            .unwrap();
        assert_eq!(state.bytes() - before, 2 * Layout::new::<DataType>().size());
        let zone: Arc<str> = Arc::from("a".repeat(8192));
        let before = state.bytes();
        state
            .inspect_data_type(
                &DataType::Timestamp(TimeUnit::Microsecond, Some(zone.clone())),
                &origins,
            )
            .unwrap();
        assert_eq!(state.bytes() - before, arc_bytes(zone.as_ref()));
        assert_eq!(
            state.inspect_data_type(
                &DataType::Dictionary(Box::new(DataType::Int64), Box::new(DataType::Utf8)),
                &origins,
            ),
            Err(RootSchemaBackingError::UnsupportedType)
        );
    }

    #[test]
    fn fields_preflight_refuses_before_unknown_field_lookup() {
        let origins = FieldMetadataOrigins::try_new(vec![], 0).unwrap();
        let fields: Fields = vec![
            Arc::new(Field::new("a", DataType::Int32, true)),
            Arc::new(Field::new("b", DataType::Int32, true)),
        ]
        .into();
        let schema = metadata(vec![]).into_schema(fields);
        let mut state = RootSchemaInspection::new(MAX_BYTES, 1, 64);
        assert_eq!(
            state.inspect_schema(schema.schema(), &schema, &origins),
            Err(RootSchemaBackingError::WorkExceeded)
        );
        assert_eq!(state.bytes(), 0);
        let oversized: Fields =
            std::iter::repeat_with(|| Arc::new(Field::new("x", DataType::Int32, true)))
                .take(RootProfileV1::MAX_COLUMNS + 1)
                .collect::<Vec<_>>()
                .into();
        let schema = metadata(vec![]).into_schema(oversized);
        assert_eq!(
            inspection().inspect_schema(schema.schema(), &schema, &origins),
            Err(RootSchemaBackingError::WorkExceeded)
        );
    }

    #[test]
    fn semantic_depth_64_is_accepted_and_65_is_rejected() {
        let leaf = field("leaf".into(), DataType::Int32);
        let mut owners = vec![leaf.clone()];
        let mut root = leaf;
        for _ in 0..64 {
            root = field("list".into(), DataType::List(root.field().clone()));
            owners.push(root.clone());
        }
        let origins = FieldMetadataOrigins::try_new(owners.clone(), 128).unwrap();
        let mut state = inspection();
        state.inspect_field(root.field(), &origins).unwrap();
        assert_eq!(state.nodes, 65);
        root = field("list".into(), DataType::List(root.field().clone()));
        owners.push(root.clone());
        let origins = FieldMetadataOrigins::try_new(owners, 128).unwrap();
        assert_eq!(
            inspection().inspect_field(root.field(), &origins),
            Err(RootSchemaBackingError::WorkExceeded)
        );
    }

    #[test]
    fn flat_4096_columns_fit_separate_tree_and_auxiliary_work_budgets() {
        let owners: Vec<_> = (0..4096)
            .map(|n| field(n.to_string(), DataType::Int32))
            .collect();
        let fields: Fields = owners.iter().map(|owner| owner.field().clone()).collect();
        let schema = metadata(vec![]).into_schema(fields);
        let origins = FieldMetadataOrigins::try_new(owners, 4096).unwrap();
        let mut state = inspection();
        state.inspect_origin_index(&origins).unwrap();
        state
            .inspect_schema(schema.schema(), &schema, &origins)
            .unwrap();
        for field in &schema.schema().fields {
            state
                .inspect_data_type(field.data_type(), &origins)
                .unwrap();
        }
        assert_eq!(state.nodes, 4096);
        assert_eq!(state.auxiliary_nodes, 16_384);
    }

    #[test]
    fn flat_4096_dictionaries_allow_physical_key_value_nodes() {
        let owners: Vec<_> = (0..4096)
            .map(|n| {
                field(
                    n.to_string(),
                    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                )
            })
            .collect();
        let fields: Fields = owners.iter().map(|owner| owner.field().clone()).collect();
        let schema = metadata(vec![]).into_schema(fields);
        let origins = FieldMetadataOrigins::try_new(owners, 4096).unwrap();
        let mut state = inspection();
        state.inspect_origin_index(&origins).unwrap();
        state
            .inspect_schema(schema.schema(), &schema, &origins)
            .unwrap();
        for field in &schema.schema().fields {
            state
                .inspect_data_type(field.data_type(), &origins)
                .unwrap();
        }
        assert_eq!(state.nodes, 12_288);
        assert_eq!(state.auxiliary_nodes, 16_384);
    }

    #[test]
    fn nested_struct_width_uses_type_work_budget_not_root_column_limit() {
        let mut owners: Vec<_> = (0..5000)
            .map(|n| field(n.to_string(), DataType::Int32))
            .collect();
        let fields: Fields = owners.iter().map(|owner| owner.field().clone()).collect();
        let root = field("struct".into(), DataType::Struct(fields));
        owners.push(root.clone());
        let schema = metadata(vec![]).into_schema(vec![root.field().clone()].into());
        let origins = FieldMetadataOrigins::try_new(owners, 8192).unwrap();
        let mut state = inspection();
        state.inspect_origin_index(&origins).unwrap();
        state
            .inspect_schema(schema.schema(), &schema, &origins)
            .unwrap();
        assert_eq!(state.nodes, 5001);
    }

    #[test]
    fn shared_aliases_are_fully_overcounted_and_auxiliary_work_is_finite() {
        let known = field("x".into(), DataType::Int32);
        let origins = FieldMetadataOrigins::try_new(vec![known.clone()], 1).unwrap();
        let mut state = inspection();
        state.inspect_field(known.field(), &origins).unwrap();
        let once = state.bytes();
        state.inspect_field(known.field(), &origins).unwrap();
        assert_eq!(state.bytes(), 2 * once);
        let mut state = inspection();
        for _ in 0..MAX_AUXILIARY_NODES {
            state.enter_auxiliary_node(0).unwrap();
        }
        assert_eq!(
            state.enter_auxiliary_node(0),
            Err(RootSchemaBackingError::WorkExceeded)
        );
    }

    #[test]
    fn generic_arc_layout_covers_aligned_payload_without_unsafe() {
        #[repr(C, align(64))]
        struct Payload([u8; 65]);
        #[repr(C, align(2))]
        struct Expected {
            strong: AtomicUsize,
            weak: AtomicUsize,
            data: Payload,
        }
        let mut state = inspection();
        state.charge_arc(&Payload([0; 65])).unwrap();
        assert_eq!(state.bytes(), Layout::new::<Expected>().size());
        // Unknown schema map/table history is irrelevant: identity refuses it.
        let known = metadata(vec![]).into_schema(Fields::empty());
        let mut unknown_map = HashMap::with_capacity(4096);
        unknown_map.insert("k".into(), "v".into());
        let unknown = Arc::new(Schema::new_with_metadata(Fields::empty(), unknown_map));
        let origins = FieldMetadataOrigins::try_new(vec![], 0).unwrap();
        assert_eq!(
            state.inspect_schema(&unknown, &known, &origins),
            Err(RootSchemaBackingError::UnknownSchemaMetadataOwner)
        );
    }
}
