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

//! Provider-owned row-mutation admission and activation contract.

// Component tests for the original source receipt and actual clone blocks.
use super::*;
use crate::connector::ConnectorPayloadRetentionGuard;
use arrow::array::{Array, Int32Array};
use arrow::datatypes::{Field, Schema};
use std::sync::Arc;

fn owned_selection() -> (ConnectorRowMutationSelection, std::sync::Weak<()>, usize) {
    let holder = Arc::new(());
    let weak = Arc::downgrade(&holder);
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let mut builder = ConnectorRowMutationSourceBuilder::try_new_with_retention(
        schema.clone(),
        ConnectorPayloadRetentionGuard::new(holder),
    )
    .unwrap();
    let array = builder
        .copy_data(0, &Int32Array::from(vec![7, 8, 9]).to_data())
        .unwrap();
    let mut columns = builder.children(1).unwrap();
    columns.push(array).unwrap();
    let source = builder.finish(3, columns).unwrap();
    let constructor_receipt = source.source_bytes();
    let selection =
        ConnectorRowMutationSelection::try_new_owned(schema, vec![source], 3, 65536).unwrap();
    (selection, weak, constructor_receipt)
}

#[test]
fn original_constructor_receipt_cannot_be_replaced_by_public_arrow_byte_count() {
    let (selection, weak, receipt) = owned_selection();
    let measured = selection.owned_source_backing_upper().unwrap().unwrap();
    // The factory's charged headers and rounded copy allocations survive as
    // their original receipt, even though Arrow exposes a smaller public size.
    assert!(measured >= receipt);
    assert!(measured as u64 > selection.byte_count());
    let legacy = ConnectorRowMutationSelection::try_new(
        selection.schema().clone(),
        selection.batches().to_vec(),
        3,
        65536,
    )
    .unwrap();
    assert_eq!(legacy.owned_source_backing_upper().unwrap(), None);
    assert!(
        legacy.batches()[0].column(0).to_data().buffers()[0].as_ptr()
            == selection.batches()[0].column(0).to_data().buffers()[0].as_ptr()
    );
    drop(selection);
    assert!(weak.upgrade().is_some());
    drop(legacy);
    assert!(weak.upgrade().is_none());
}

#[test]
fn actual_selection_clone_allocates_headers_and_shares_original_source_until_last_buffer() {
    let (selection, weak, _) = owned_selection();
    let clone = selection.clone();
    assert!(Arc::ptr_eq(&selection.schema, &clone.schema));
    assert!(Arc::ptr_eq(
        selection.source_ownership.as_ref().unwrap(),
        clone.source_ownership.as_ref().unwrap()
    ));
    assert_ne!(selection.batches.as_ptr(), clone.batches.as_ptr());
    assert_ne!(
        selection.batch_row_offsets.as_ptr(),
        clone.batch_row_offsets.as_ptr()
    );
    assert_ne!(selection.retention.as_ptr(), clone.retention.as_ptr());
    assert_ne!(
        selection.batches()[0].columns().as_ptr(),
        clone.batches()[0].columns().as_ptr()
    );
    assert!(Arc::ptr_eq(
        selection.batches()[0].column(0),
        clone.batches()[0].column(0)
    ));
    assert_eq!(
        selection.owned_source_backing_upper().unwrap(),
        clone.owned_source_backing_upper().unwrap()
    );
    assert!(
        selection.cloned_container_bytes().unwrap()
            > std::mem::size_of::<ConnectorRowMutationSelection>()
    );
    let last_buffer = clone.batches()[0].column(0).to_data().buffers()[0].slice_with_length(4, 4);
    drop(selection);
    assert!(weak.upgrade().is_some());
    drop(clone);
    assert!(weak.upgrade().is_some());
    assert_eq!(last_buffer.as_slice(), 8_i32.to_ne_bytes());
    drop(last_buffer);
    assert!(weak.upgrade().is_none());
}

#[test]
fn spare_original_container_capacity_does_not_become_shared_source_or_fresh_clone_capacity() {
    let (mut selection, weak, _) = owned_selection();
    let source = selection.owned_source_backing_upper().unwrap();
    let fresh = selection.cloned_container_bytes().unwrap();
    let before = selection.retained_container_bytes().unwrap();
    selection.batches.reserve_exact(31);
    selection.batch_row_offsets.reserve_exact(31);
    selection.retention.reserve_exact(31);
    let after = selection.retained_container_bytes().unwrap();
    assert!(after > before);
    assert_eq!(selection.owned_source_backing_upper().unwrap(), source);
    assert_eq!(selection.cloned_container_bytes().unwrap(), fresh);
    let clone = selection.clone();
    assert!(clone.batches.capacity() < selection.batches.capacity());
    assert!(clone.batch_row_offsets.capacity() < selection.batch_row_offsets.capacity());
    assert!(clone.retention.capacity() < selection.retention.capacity());
    drop(selection);
    assert!(weak.upgrade().is_some());
    drop(clone);
    assert!(weak.upgrade().is_none());
}
