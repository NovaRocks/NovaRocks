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

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use novarocks_types::arrow_metadata_owner::{
    ArrowMetadataOwner, MetadataOwnerError, MetadataOwnerLimits,
};

thread_local! {
    static TRACK: Cell<bool> = const { Cell::new(false) };
    static CALLS: Cell<usize> = const { Cell::new(0) };
    static BYTES: Cell<usize> = const { Cell::new(0) };
    static FREED: Cell<usize> = const { Cell::new(0) };
}
struct Probe;
fn record(bytes: usize) {
    if TRACK.try_with(Cell::get).unwrap_or(false) {
        let _ = CALLS.try_with(|value| value.set(value.get() + 1));
        let _ = BYTES.try_with(|value| value.set(value.get() + bytes));
    }
}
// SAFETY: Every request is delegated unchanged to System; observations use
// only allocation-free thread-local integer cells.
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
        if TRACK.try_with(Cell::get).unwrap_or(false) {
            let _ = FREED.try_with(|value| value.set(value.get() + layout.size()));
        }
        unsafe { System.dealloc(ptr, layout) }
    }
}
#[global_allocator]
static ALLOCATOR: Probe = Probe;
fn tracked<T>(f: impl FnOnce() -> T) -> (T, usize, usize) {
    CALLS.with(|value| value.set(0));
    BYTES.with(|value| value.set(0));
    FREED.with(|value| value.set(0));
    TRACK.with(|value| value.set(true));
    let result = f();
    TRACK.with(|value| value.set(false));
    (result, CALLS.with(Cell::get), BYTES.with(Cell::get))
}
fn limits() -> MetadataOwnerLimits {
    MetadataOwnerLimits {
        entries: 65536,
        construction_bytes: 16 * 1024 * 1024,
    }
}
fn entries(count: usize) -> Vec<(String, String)> {
    (0..count)
        .map(|index| (index.to_string(), "value".to_string()))
        .collect()
}
#[test]
fn genuine_btree_metadata_and_arc_are_prepaid_at_all_growth_boundaries() {
    for count in [
        0, 1, 3, 4, 7, 8, 11, 12, 14, 15, 28, 29, 56, 57, 85, 86, 143, 144, 4096,
    ] {
        let values = entries(count);
        let vector_bytes = values.capacity() * std::mem::size_of::<(String, String)>();
        let string_bytes = values
            .iter()
            .map(|(k, v)| k.capacity() + v.capacity())
            .sum::<usize>();
        let (peak, calls, _) = tracked(|| ArrowMetadataOwner::preflight(&values, limits()));
        assert_eq!(calls, 0, "preflight must not allocate");
        let peak = peak.unwrap();
        let (owner, calls, actual_table_bytes) =
            tracked(|| ArrowMetadataOwner::try_new(values, limits()));
        let owner = owner.unwrap();
        let conversion =
            novarocks_type_contract::owned_resources::arrow_metadata::hashmap_conversion_requests(
                count,
            )
            .unwrap();
        let requests =
            usize::from(count != 0) + conversion.allocation_requests_upper_bound().unwrap();
        assert_eq!(
            calls, requests,
            "the original reserve and each Arrow conversion request are separate"
        );
        // The observed frees include the input Vec allocated before tracking.
        // Keys/values moved into Metadata and remain live; add their unchanged
        // input capacities separately. Temporary tables/Vec/sort heaps are gone.
        let actual_live_tree_and_arc = actual_table_bytes + vector_bytes - FREED.with(Cell::get);
        assert!(
            owner.backing_bytes() >= actual_live_tree_and_arc + string_bytes,
            "count={count}"
        );
        assert!(
            peak >= actual_table_bytes + string_bytes + vector_bytes,
            "count={count}"
        );
        assert_eq!(owner.construction_bytes(), peak);
        assert_eq!(owner.metadata().len(), count);
    }
}
#[test]
fn source_vector_spare_and_string_spare_are_preflighted_before_allocation() {
    let mut values = Vec::with_capacity(8192);
    let mut key = String::with_capacity(32768);
    key.push('k');
    let mut value = String::with_capacity(32768);
    value.push('v');
    values.push((key, value));
    let (result, calls, _) = tracked(|| {
        ArrowMetadataOwner::try_new(
            values,
            MetadataOwnerLimits {
                entries: 1,
                construction_bytes: 4096,
            },
        )
    });
    assert_eq!(result.unwrap_err(), MetadataOwnerError::CapacityExceeded);
    assert_eq!(calls, 0);
}
#[test]
fn entry_rejection_and_duplicate_failure_do_not_publish_a_receipt() {
    let values = entries(1);
    let (result, calls, _) = tracked(|| {
        ArrowMetadataOwner::try_new(
            values,
            MetadataOwnerLimits {
                entries: 0,
                construction_bytes: usize::MAX,
            },
        )
    });
    assert_eq!(result.unwrap_err(), MetadataOwnerError::CapacityExceeded);
    assert_eq!(calls, 0);
    let values = vec![
        ("k".to_string(), "a".to_string()),
        ("k".to_string(), "b".to_string()),
    ];
    let (result, calls, _) = tracked(|| ArrowMetadataOwner::try_new(values, limits()));
    assert_eq!(result.unwrap_err(), MetadataOwnerError::DuplicateKey);
    assert_eq!(
        calls, 1,
        "the duplicate is rejected before Arrow consumes the original HashMap"
    );
}
#[test]
fn exact_field_owner_survives_aliases_but_structural_clones_and_mutation_lose_proof() {
    let metadata = ArrowMetadataOwner::try_new(entries(3), limits()).unwrap();
    let bytes = metadata.backing_bytes();
    let owner = metadata.into_field("field".to_string(), DataType::Int32, true);
    let alias = owner.clone();
    assert_eq!(tracked(|| owner.backing_bytes_for(alias.field())).1, 0);
    assert_eq!(owner.backing_bytes_for(alias.field()), Some(bytes));
    let equal = Arc::new(owner.field().as_ref().clone());
    assert_eq!(equal.as_ref(), owner.field().as_ref());
    assert_eq!(owner.backing_bytes_for(&equal), None);
    let mut mutable = Arc::clone(owner.field());
    Arc::make_mut(&mut mutable).metadata_mut().clear();
    assert_eq!(owner.backing_bytes_for(&mutable), None);
    assert_eq!(owner.field().metadata().len(), 3);
}
#[test]
fn exact_schema_receipt_does_not_cover_a_structurally_equal_unknown_map() {
    let metadata = ArrowMetadataOwner::try_new(entries(1), limits()).unwrap();
    let bytes = metadata.backing_bytes();
    let owner = metadata.into_schema(vec![Arc::new(Field::new("v", DataType::Int32, true))].into());
    assert_eq!(owner.backing_bytes_for(owner.schema()), Some(bytes));
    let mut unknown = std::collections::HashMap::with_capacity(32768);
    unknown.insert("0".to_string(), "value".to_string());
    let equal = Arc::new(Schema::new_with_metadata(
        owner.schema().fields.clone(),
        unknown,
    ));
    assert_eq!(owner.schema().as_ref(), equal.as_ref());
    assert_eq!(tracked(|| owner.backing_bytes_for(&equal)).1, 0);
    assert_eq!(owner.backing_bytes_for(&equal), None);
}
