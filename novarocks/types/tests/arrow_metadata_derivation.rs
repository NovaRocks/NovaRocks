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

//! These probes prove exact metadata-map provenance and finite traversal only.
//! Field names, DataType allocations, Arc/wrapper scaffolds and complete source
//! construction peaks are deliberately outside the metadata receipt's claim.
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef, Fields, Schema};
use novarocks_types::arrow_metadata_owner::{
    ArrowMetadataOwner, FieldMetadataOrigins, MetadataOwnedField, MetadataOwnerError,
    MetadataOwnerLimits,
};

thread_local! {
    static TRACK: Cell<bool> = const { Cell::new(false) };
    static CALLS: Cell<usize> = const { Cell::new(0) };
    static BYTES: Cell<usize> = const { Cell::new(0) };
}
struct Probe;
fn record(bytes: usize) {
    if TRACK.try_with(Cell::get).unwrap_or(false) {
        let _ = CALLS.try_with(|value| value.set(value.get() + 1));
        let _ = BYTES.try_with(|value| value.set(value.get() + bytes));
    }
}
// SAFETY: Requests are forwarded unchanged to System. The probe observes
// numeric allocation sizes with thread-local cells and never dereferences them.
unsafe impl GlobalAlloc for Probe {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        record(size);
        unsafe { System.realloc(ptr, layout, size) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}
#[global_allocator]
static ALLOCATOR: Probe = Probe;
fn tracked<T>(f: impl FnOnce() -> T) -> (T, usize, usize) {
    CALLS.with(|value| value.set(0));
    BYTES.with(|value| value.set(0));
    TRACK.with(|value| value.set(true));
    let result = f();
    TRACK.with(|value| value.set(false));
    (result, CALLS.with(Cell::get), BYTES.with(Cell::get))
}
fn limits() -> MetadataOwnerLimits {
    MetadataOwnerLimits {
        entries: 64,
        construction_bytes: 1024 * 1024,
    }
}
fn metadata() -> ArrowMetadataOwner {
    ArrowMetadataOwner::try_new(
        vec![
            ("logical_type".into(), "JSON".into()),
            ("source".into(), "中文/metadata".into()),
        ],
        limits(),
    )
    .unwrap()
}
fn field(name: &str, data_type: DataType) -> MetadataOwnedField {
    metadata().into_field(name.into(), data_type, true)
}
fn pointers(origins: &FieldMetadataOrigins) -> Vec<usize> {
    origins
        .owners()
        .iter()
        .map(|owner| Arc::as_ptr(owner.field()) as usize)
        .collect()
}
fn assert_exact_owners(origins: &FieldMetadataOrigins, fields: &[FieldRef]) {
    let mut expected = fields
        .iter()
        .map(|field| Arc::as_ptr(field) as usize)
        .collect::<Vec<_>>();
    expected.sort_unstable();
    expected.dedup();
    assert_eq!(pointers(origins), expected);
    for field in fields {
        let owner = origins.owner_for(field).unwrap();
        assert!(Arc::ptr_eq(owner.field(), field));
        assert_eq!(
            origins.metadata_bytes_for(field),
            owner.backing_bytes_for(field)
        );
    }
}

#[test]
fn derived_field_preserves_metadata_with_a_new_exact_arc_identity() {
    let source = field("source", DataType::Utf8);
    let alias = source.clone();
    assert!(Arc::ptr_eq(source.field(), alias.field()));
    let derived = source
        .derive_field(DataType::LargeUtf8, false, limits())
        .unwrap();
    assert!(!Arc::ptr_eq(source.field(), derived.field()));
    assert_eq!(derived.field().metadata(), source.field().metadata());
    assert_eq!(derived.field().name(), "source");
    assert_eq!(derived.field().data_type(), &DataType::LargeUtf8);
    assert!(!derived.field().is_nullable());
    assert_eq!(source.backing_bytes_for(derived.field()), None);
    assert_eq!(derived.backing_bytes_for(source.field()), None);
    assert!(derived.backing_bytes_for(derived.field()).unwrap() > 0);
    assert!(Arc::ptr_eq(
        source.field().metadata().as_arc().unwrap(),
        derived.field().metadata().as_arc().unwrap()
    ));
    for (key, value) in source.field().metadata() {
        let (derived_key, derived_value) = derived
            .field()
            .metadata()
            .iter()
            .find(|(candidate, _)| *candidate == key)
            .unwrap();
        assert_eq!(key.as_ptr(), derived_key.as_ptr());
        assert_eq!(value.as_ptr(), derived_value.as_ptr());
    }
    let structural_copy = Arc::new(source.field().as_ref().clone());
    assert_eq!(structural_copy.as_ref(), source.field().as_ref());
    assert_eq!(source.backing_bytes_for(&structural_copy), None);
    let mut copy_on_write = Arc::clone(source.field());
    Arc::make_mut(&mut copy_on_write).set_nullable(false);
    assert_eq!(source.backing_bytes_for(&copy_on_write), None);
    assert!(source.field().is_nullable());
}

#[test]
fn derived_schema_preserves_only_top_metadata_and_keeps_exact_field_arcs() {
    let first = field("first", DataType::Int32);
    let second = field("second", DataType::Utf8);
    let source = metadata().into_schema(vec![Arc::clone(first.field())].into());
    let alias = source.clone();
    assert!(Arc::ptr_eq(source.schema(), alias.schema()));
    let derived = source
        .derive_schema(vec![Arc::clone(second.field())].into(), limits())
        .unwrap();
    assert!(!Arc::ptr_eq(source.schema(), derived.schema()));
    assert_eq!(source.schema().metadata(), derived.schema().metadata());
    assert!(Arc::ptr_eq(&derived.schema().fields()[0], second.field()));
    assert!(Arc::ptr_eq(&source.schema().fields()[0], first.field()));
    assert_eq!(source.backing_bytes_for(derived.schema()), None);
    assert_eq!(derived.backing_bytes_for(source.schema()), None);
    assert!(Arc::ptr_eq(
        source.schema().metadata().as_arc().unwrap(),
        derived.schema().metadata().as_arc().unwrap()
    ));
    let structural_copy = Arc::new(Schema::clone(source.schema().as_ref()));
    assert_eq!(source.backing_bytes_for(&structural_copy), None);
    for (key, value) in source.schema().metadata() {
        let (derived_key, derived_value) = derived
            .schema()
            .metadata()
            .iter()
            .find(|(candidate, _)| *candidate == key)
            .unwrap();
        assert_eq!(key.as_ptr(), derived_key.as_ptr());
        assert_eq!(value.as_ptr(), derived_value.as_ptr());
    }
}

#[test]
fn field_and_schema_derivation_reject_before_any_metadata_copy_allocation() {
    let source = field("source", DataType::Int32);
    let schema = metadata().into_schema(vec![Arc::clone(source.field())].into());
    for bound in [
        MetadataOwnerLimits {
            entries: 1,
            construction_bytes: usize::MAX,
        },
        MetadataOwnerLimits {
            entries: 64,
            construction_bytes: 0,
        },
    ] {
        let (result, calls, bytes) = tracked(|| source.derive_field(DataType::Int32, false, bound));
        assert_eq!(result.unwrap_err(), MetadataOwnerError::CapacityExceeded);
        assert_eq!((calls, bytes), (0, 0));
        let fields: Fields = vec![Arc::clone(source.field())].into();
        let (result, calls, bytes) = tracked(|| schema.derive_schema(fields, bound));
        assert_eq!(result.unwrap_err(), MetadataOwnerError::CapacityExceeded);
        assert_eq!((calls, bytes), (0, 0));
    }
    // Rejection leaves original maps and their exact owner tokens intact.
    assert_eq!(source.field().metadata().len(), 2);
    assert!(source.backing_bytes_for(source.field()).is_some());
    assert_eq!(schema.schema().metadata().len(), 2);
}

#[test]
fn origins_sort_dedup_and_lookup_by_exact_identity_without_copying_maps() {
    let a = field("a", DataType::Int32);
    let b = field("b", DataType::Int32);
    let owners = vec![b.clone(), a.clone(), b.clone(), a.clone()];
    let origins = FieldMetadataOrigins::try_new(owners, 4).unwrap();
    assert_exact_owners(&origins, &[Arc::clone(a.field()), Arc::clone(b.field())]);
    let structural_copy = Arc::new(a.field().as_ref().clone());
    let (found, calls, bytes) = tracked(|| {
        (
            origins.owner_for(a.field()).is_some(),
            origins.metadata_bytes_for(&structural_copy),
        )
    });
    assert_eq!(found, (true, None));
    assert_eq!((calls, bytes), (0, 0));
    // The input work bound is checked before deduplication.
    assert_eq!(
        FieldMetadataOrigins::try_new(vec![a.clone(), a], 1).unwrap_err(),
        MetadataOwnerError::CapacityExceeded,
    );
}

#[test]
fn narrowing_tree_retains_only_reachable_exact_tokens_and_dedups_shared_child() {
    let child = field("child", DataType::Utf8);
    let unrelated = field("unrelated", DataType::Int8);
    let root = field(
        "root",
        DataType::Struct(vec![Arc::clone(child.field()), Arc::clone(child.field())].into()),
    );
    let origins =
        FieldMetadataOrigins::try_new(vec![root.clone(), unrelated.clone(), child.clone()], 3)
            .unwrap();
    let narrowed = origins.for_field_tree(root.field(), 3, 1).unwrap();
    assert_exact_owners(
        &narrowed,
        &[Arc::clone(root.field()), Arc::clone(child.field())],
    );
    assert!(narrowed.owner_for(unrelated.field()).is_none());
    assert_eq!(
        origins.for_field_tree(root.field(), 2, 1).unwrap_err(),
        MetadataOwnerError::CapacityExceeded
    );
    let leaf = origins.for_field_tree(child.field(), 1, 0).unwrap();
    assert_exact_owners(&leaf, &[Arc::clone(child.field())]);
}

#[test]
fn replacing_root_changes_only_its_token_and_preserves_child_receipts() {
    let child = field("child", DataType::Int32);
    let root = field("root", DataType::List(Arc::clone(child.field())));
    let origins = FieldMetadataOrigins::try_new(vec![root.clone(), child.clone()], 2).unwrap();
    let replacement = root
        .derive_field(root.field().data_type().clone(), false, limits())
        .unwrap();
    let replacement_arc = Arc::clone(replacement.field());
    let replaced = origins
        .replacing_root(root.field(), replacement, 2)
        .unwrap();
    assert_exact_owners(&replaced, &[replacement_arc, Arc::clone(child.field())]);
    assert!(replaced.owner_for(root.field()).is_none());
    assert!(origins.owner_for(root.field()).is_some());
}

#[test]
fn unknown_child_and_structurally_equal_child_refuse_provenance() {
    let child = field("child", DataType::Utf8);
    for unknown in [
        Arc::new(Field::new("unknown", DataType::Utf8, true)),
        Arc::new(child.field().as_ref().clone()),
    ] {
        let root = field("root", DataType::List(unknown));
        let origins = FieldMetadataOrigins::try_new(vec![root.clone(), child.clone()], 2).unwrap();
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        let error = origins.for_field_tree(root.field(), 2, 1).unwrap_err();
        assert_eq!(error, MetadataOwnerError::MissingFieldOrigin);
        assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
        assert_eq!(error.failure_fact().reason().code(), 4);
        assert!(error.failure_fact().resource().is_none());
        assert_eq!(
            origins.for_field_tree(root.field(), 65536, 64).unwrap_err(),
            MetadataOwnerError::MissingFieldOrigin
        );
        assert_eq!(
            origins.for_field_tree(root.field(), 0, 1).unwrap_err(),
            MetadataOwnerError::CapacityExceeded
        );
    }
}

fn wrap_lists(
    mut root: MetadataOwnedField,
    count: usize,
    owners: &mut Vec<MetadataOwnedField>,
) -> MetadataOwnedField {
    owners.push(root.clone());
    for _ in 0..count {
        root = field("list", DataType::List(Arc::clone(root.field())));
        owners.push(root.clone());
    }
    root
}

#[test]
fn map_entries_wrapper_does_not_consume_an_extra_semantic_depth() {
    let key = field("key", DataType::Utf8);
    let value = field("value", DataType::Int32);
    let entries = field(
        "entries",
        DataType::Struct(vec![Arc::clone(key.field()), Arc::clone(value.field())].into()),
    );
    let map = field("map", DataType::Map(Arc::clone(entries.field()), false));
    let mut owners = vec![key, value, entries];
    // 63 List levels put Map/entries at 63 and key/value at semantic depth 64.
    let root = wrap_lists(map, 63, &mut owners);
    let fields = owners
        .iter()
        .map(|owner| Arc::clone(owner.field()))
        .collect::<Vec<_>>();
    let origins = FieldMetadataOrigins::try_new(owners, 67).unwrap();
    let narrowed = origins.for_field_tree(root.field(), 67, 64).unwrap();
    assert_exact_owners(&narrowed, &fields);
    assert_eq!(
        origins.for_field_tree(root.field(), 67, 63).unwrap_err(),
        MetadataOwnerError::CapacityExceeded
    );
}

#[test]
fn dictionary_scalar_physical_leaves_are_allowed_at_semantic_depth_limit() {
    let dictionary = field(
        "dictionary",
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
    );
    let mut owners = Vec::new();
    let root = wrap_lists(dictionary, 64, &mut owners);
    let fields = owners
        .iter()
        .map(|owner| Arc::clone(owner.field()))
        .collect::<Vec<_>>();
    let origins = FieldMetadataOrigins::try_new(owners, 67).unwrap();
    assert_exact_owners(
        &origins.for_field_tree(root.field(), 67, 64).unwrap(),
        &fields,
    );
    assert_eq!(
        origins.for_field_tree(root.field(), 67, 63).unwrap_err(),
        MetadataOwnerError::CapacityExceeded
    );
}

#[test]
fn dictionary_nested_or_composite_value_cannot_bypass_depth_limit() {
    let child = field("child", DataType::Utf8);
    for value in [
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        DataType::List(Arc::clone(child.field())),
        DataType::Struct(vec![Arc::clone(child.field())].into()),
    ] {
        let dictionary = field(
            "dictionary",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(value)),
        );
        let mut owners = vec![child.clone()];
        let root = wrap_lists(dictionary, 64, &mut owners);
        let origins = FieldMetadataOrigins::try_new(owners, 128).unwrap();
        assert_eq!(
            origins.for_field_tree(root.field(), 128, 64).unwrap_err(),
            MetadataOwnerError::CapacityExceeded
        );
    }
}

fn dictionary_tree(depth: usize) -> DataType {
    if depth == 0 {
        return DataType::Int32;
    }
    DataType::Dictionary(
        Box::new(dictionary_tree(depth - 1)),
        Box::new(dictionary_tree(depth - 1)),
    )
}

#[test]
fn type_node_work_budget_bounds_dictionary_traversal_independently_of_field_count() {
    let simple = field(
        "simple",
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
    );
    let origins = FieldMetadataOrigins::try_new(vec![simple.clone()], 1).unwrap();
    assert_eq!(
        origins.for_field_tree(simple.field(), 1, 64).unwrap_err(),
        MetadataOwnerError::CapacityExceeded
    );
    assert_eq!(
        origins.for_field_tree(simple.field(), 2, 64).unwrap_err(),
        MetadataOwnerError::CapacityExceeded
    );
    assert_exact_owners(
        &origins.for_field_tree(simple.field(), 3, 0).unwrap(),
        &[Arc::clone(simple.field())],
    );
    // This physical binary tree has 127 types but only one genuine Field owner.
    let root = field("tree", dictionary_tree(6));
    let origins = FieldMetadataOrigins::try_new(vec![root.clone()], 1).unwrap();
    assert_eq!(
        origins.for_field_tree(root.field(), 1, 64).unwrap_err(),
        MetadataOwnerError::CapacityExceeded
    );
    assert_eq!(
        origins.for_field_tree(root.field(), 126, 64).unwrap_err(),
        MetadataOwnerError::CapacityExceeded
    );
    assert_exact_owners(
        &origins.for_field_tree(root.field(), 127, 64).unwrap(),
        &[Arc::clone(root.field())],
    );
}
