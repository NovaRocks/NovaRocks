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

//! Arrow capacity semantics (MEM-1 acceptance A04 and A05).
//!
//! A slice, a clone, an exported array, a shared null bitmap, a dictionary's
//! values and a `MutableBuffer` conversion all keep the same backing alive, so
//! the charge must last as long as the backing and be counted exactly once.
//! The case that matters most is the third-party owner: 64 MiB retained behind
//! a 1 KiB view must be charged for what it retains, while the view is alive
//! and not only checked after everything has been dropped.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, DictionaryArray, Int32Array, Int64Array, RecordBatch, StringArray, StructArray,
};
use arrow::datatypes::{DataType, Field, Int32Type, Schema};
use arrow_buffer::{Buffer, MutableBuffer, NullBuffer};
use novarocks_memory::account::{AccountHandle, TopUpPolicy};
use novarocks_memory::authority::{AuthorityConfig, MemoryAuthority};
use novarocks_memory::ids::{AccountKind, ExternalRef};
use novarocks_memory_arrow::{FulfilmentPool, claim_array_data, claim_batch, claim_buffer};

const MIB: u64 = 1024 * 1024;

fn fixture(capacity: u64) -> (MemoryAuthority, AccountHandle) {
    let mut config = AuthorityConfig::new(capacity * 4, capacity, capacity);
    config.top_up = TopUpPolicy::uniform(1);
    let authority = MemoryAuthority::new(config).expect("valid configuration");
    let work = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(1))
        .expect("work account");
    (authority, work)
}

fn pool(work: &AccountHandle, bytes: u64) -> FulfilmentPool {
    FulfilmentPool::new(work.request_grant(bytes).expect("issued capacity"))
}

#[test]
fn a_claim_charges_the_whole_allocation_not_the_visible_length() {
    let (_authority, work) = fixture(64 * MIB);
    let pool = pool(&work, 8 * MIB);

    let buffer = Buffer::from_vec(vec![0u8; 4096]);
    let capacity = buffer.capacity() as u64;
    let receipt = claim_buffer(&buffer, &pool);

    assert_eq!(receipt.len(), 1);
    assert_eq!(receipt.charged_bytes(), capacity);
    assert_eq!(work.snapshot().live_bytes, capacity);
}

#[test]
fn a_large_owner_behind_a_small_view_is_charged_while_the_view_is_alive() {
    // The counterexample MEM-1 names: a third party retains 64 MiB and hands
    // out a 1 KiB slice. Charging the visible length would under-report for as
    // long as the slice lives, and checking only after the last drop would
    // miss it entirely.
    let (_authority, work) = fixture(256 * MIB);
    let pool = pool(&work, 128 * MIB);

    let retained = Buffer::from_vec(vec![7u8; 64 * MIB as usize]);
    let capacity = retained.capacity() as u64;
    assert!(capacity >= 64 * MIB);

    let receipt = claim_buffer(&retained, &pool);
    let view = retained.slice_with_length(0, 1024);
    assert_eq!(view.len(), 1024);

    // Drop the original handle: the slice still keeps the whole allocation
    // alive, so the charge must stay at the full capacity.
    drop(retained);
    assert_eq!(
        work.snapshot().live_bytes,
        capacity,
        "the retained backing is still charged behind a 1 KiB view"
    );
    assert_eq!(receipt.charged_bytes(), capacity);

    // Only the last alias releases it.
    drop(view);
    assert_eq!(work.snapshot().live_bytes, 0);
    assert_eq!(receipt.charged_bytes(), 0);
}

#[test]
fn slices_and_clones_share_one_charge() {
    let (_authority, work) = fixture(64 * MIB);
    let pool = pool(&work, 8 * MIB);

    let buffer = Buffer::from_vec(vec![0u8; 8192]);
    let capacity = buffer.capacity() as u64;
    claim_buffer(&buffer, &pool);

    let aliases: Vec<Buffer> = (0..16)
        .map(|index| buffer.slice_with_length(index * 16, 64))
        .chain(std::iter::repeat_n(buffer.clone(), 16))
        .collect();
    assert_eq!(
        work.snapshot().live_bytes,
        capacity,
        "32 aliases of one allocation are charged once"
    );

    drop(aliases);
    assert_eq!(work.snapshot().live_bytes, capacity, "the original remains");
    drop(buffer);
    assert_eq!(work.snapshot().live_bytes, 0);
}

#[test]
fn a_shared_null_bitmap_and_dictionary_values_are_charged_once() {
    let (_authority, work) = fixture(64 * MIB);
    let pool = pool(&work, 16 * MIB);

    // Two columns built over one null bitmap allocation.
    let nulls = NullBuffer::from(vec![true, false, true, true]);
    let left = Int32Array::new(vec![1, 2, 3, 4].into(), Some(nulls.clone()));
    let right = Int64Array::new(vec![5, 6, 7, 8].into(), Some(nulls.clone()));

    let dictionary: DictionaryArray<Int32Type> = vec!["a", "b", "a", "b"].into_iter().collect();

    let schema = Arc::new(Schema::new(vec![
        Field::new("left", DataType::Int32, true),
        Field::new("right", DataType::Int64, true),
        Field::new(
            "dictionary",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(left) as ArrayRef,
            Arc::new(right) as ArrayRef,
            Arc::new(dictionary) as ArrayRef,
        ],
    )
    .expect("batch");

    let receipt = claim_batch(&batch, &pool);
    let charged = work.snapshot().live_bytes;

    // Every distinct backing appears once. Claiming again over the same batch
    // must not raise the total, which is what proves the de-duplication is by
    // allocation identity rather than by column.
    let second = claim_batch(&batch, &pool);
    assert_eq!(second.len(), receipt.len());
    assert_eq!(
        work.snapshot().live_bytes,
        charged,
        "re-claiming the same batch does not accumulate charges"
    );

    // The test itself still holds the null bitmap, so exactly that backing
    // stays charged when the batch goes away. Asserting the precise residual
    // is stronger than asserting zero: it shows the charge follows the
    // backing's real last reference rather than the wrapper that measured it.
    let null_capacity = nulls.inner().inner().capacity() as u64;
    drop(batch);
    assert_eq!(
        work.snapshot().live_bytes,
        null_capacity,
        "the bitmap the test still holds is still charged, and nothing else is"
    );

    drop(nulls);
    assert_eq!(
        work.snapshot().live_bytes,
        0,
        "every charge settles with its backing"
    );
}

#[test]
fn a_nested_struct_is_charged_through_its_children() {
    let (_authority, work) = fixture(64 * MIB);
    let pool = pool(&work, 16 * MIB);

    let values = Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef;
    let names = Arc::new(StringArray::from(vec!["a", "b", "c", "d"])) as ArrayRef;
    let nested = StructArray::from(vec![
        (
            Arc::new(Field::new("value", DataType::Int32, false)),
            values,
        ),
        (Arc::new(Field::new("name", DataType::Utf8, false)), names),
    ]);

    let data = nested.to_data();
    let receipt = claim_array_data(&data, &pool);
    assert!(
        receipt.len() >= 3,
        "child value, offset and data buffers are all charged: {}",
        receipt.len()
    );
    assert!(work.snapshot().live_bytes > 0);

    drop(data);
    drop(nested);
    assert_eq!(work.snapshot().live_bytes, 0);
}

#[test]
fn a_mutable_buffer_reservation_is_not_evidence_of_retained_capacity() {
    // This is the 58.2.0 behaviour the adapter refuses to trust: truncating a
    // MutableBuffer shrinks its reservation to the logical length while the
    // allocation keeps its capacity. A charge taken at a handover point, on
    // the immutable buffer, reports the capacity that is really retained.
    let (_authority, work) = fixture(64 * MIB);
    let pool = pool(&work, 8 * MIB);

    let mut mutable = MutableBuffer::with_capacity(4096);
    mutable.extend_from_slice(&[1u8; 4096]);
    let grown_capacity = mutable.capacity();
    mutable.truncate(40);
    assert_eq!(
        mutable.capacity(),
        grown_capacity,
        "truncate keeps the allocation"
    );

    let frozen: Buffer = mutable.into();
    let receipt = claim_buffer(&frozen, &pool);
    assert_eq!(
        receipt.charged_bytes(),
        frozen.capacity() as u64,
        "the charge is the retained capacity, not the 40 visible bytes"
    );
    assert!(receipt.charged_bytes() >= 4096);

    drop(frozen);
    assert_eq!(work.snapshot().live_bytes, 0);
}

#[test]
fn a_charge_survives_being_exported_as_a_standalone_array() {
    let (_authority, work) = fixture(64 * MIB);
    let pool = pool(&work, 16 * MIB);

    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int32,
        false,
    )]));
    let column = Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef;
    let batch = RecordBatch::try_new(schema, vec![Arc::clone(&column)]).expect("batch");
    claim_batch(&batch, &pool);
    let charged = work.snapshot().live_bytes;
    assert!(charged > 0);

    // The batch goes away but the exported column keeps the backing alive.
    drop(batch);
    assert_eq!(
        work.snapshot().live_bytes,
        charged,
        "an exported array keeps its backing charged"
    );

    drop(column);
    assert_eq!(work.snapshot().live_bytes, 0);
}

#[test]
fn a_claim_is_released_exactly_once_across_many_aliases() {
    let (authority, work) = fixture(64 * MIB);
    let pool = pool(&work, 8 * MIB);

    let buffer = Buffer::from_vec(vec![0u8; 16 * 1024]);
    let capacity = buffer.capacity() as u64;
    claim_buffer(&buffer, &pool);

    let mut aliases = Vec::new();
    for _ in 0..64 {
        aliases.push(buffer.clone());
    }
    while let Some(alias) = aliases.pop() {
        drop(alias);
        assert_eq!(
            work.snapshot().live_bytes,
            capacity,
            "dropping an alias is not a release"
        );
    }
    drop(buffer);
    assert_eq!(work.snapshot().live_bytes, 0);
    assert!(authority.snapshot().honours_capacity_bound());
}
