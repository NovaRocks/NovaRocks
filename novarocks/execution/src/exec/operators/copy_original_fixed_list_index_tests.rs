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

//! Arrow60.1 generates checked UInt32 child indices from usize offsets.
//! NullArray permits wide logical extents without allocating value payload.
use super::*;
use arrow::array::{FixedSizeListArray, NullArray};

#[cfg(target_pointer_width = "64")]
fn original_wide_fixed_list(row: usize) -> (ArrayRef, Arc<UInt64Array>) {
    let rows = row.checked_add(1).unwrap();
    let child_len = rows.checked_mul(2).unwrap();
    let source: ArrayRef = Arc::new(
        FixedSizeListArray::try_new(
            Arc::new(Field::new("original_null_child", DataType::Null, true)),
            2,
            Arc::new(NullArray::new(child_len)),
            None,
        )
        .unwrap(),
    );
    (source, Arc::new(UInt64Array::from(vec![row as u64])))
}

#[cfg(target_pointer_width = "64")]
fn check_original_wide_fixed_list(row: usize) {
    let (source, indices) = original_wide_fixed_list(row);
    let original = arrow::compute::take(source.as_ref(), indices.as_ref(), None).unwrap();
    assert_eq!(original.len(), 1);
    let tr = tracker();
    let host = Host::new(tr.clone(), None);
    let control = Control::new(tr, None);
    let actual = take_copy_in(
        source,
        CopyIndices::UInt64(indices),
        (),
        host.clone(),
        &control,
    )
    .unwrap();
    assert_eq!(actual.values().to_data(), original.to_data());
    drop(actual);
    assert_released(&host);
}

#[cfg(target_pointer_width = "64")]
#[test]
fn by_copy_original_fixed_list_offset_above_i32_stays_valid_u32() {
    let row = (i32::MAX as usize / 2) + 1;
    let (source, _) = original_wide_fixed_list(row);
    let source = source
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .unwrap();
    assert_eq!(source.value_offset_at(row), 2_147_483_648);
    check_original_wide_fixed_list(row);
}

#[cfg(target_pointer_width = "64")]
#[test]
fn by_copy_original_fixed_list_offset_above_u32_preserves_actual_sdk_error() {
    let row = (u32::MAX as usize).div_ceil(2);
    let (source, indices) = original_wide_fixed_list(row);
    let list = source
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .unwrap();
    assert_eq!(list.value_offset_at(row), 4_294_967_296);
    let expected = arrow::compute::take(source.as_ref(), indices.as_ref(), None).unwrap_err();
    assert_eq!(
        expected.to_string(),
        "Compute error: FixedSizeList offset overflows u32"
    );
    let tr = tracker();
    let host = Host::new(tr.clone(), None);
    let control = Control::new(tr, None);
    match take_copy_in(
        source,
        CopyIndices::UInt64(indices),
        (),
        host.clone(),
        &control,
    ) {
        Err(CopyOperationError::OriginalData(actual)) => {
            assert_eq!(actual, expected.to_string());
            drop(actual);
        }
        Ok(_) => panic!("overflow must retain actual SDK error"),
        Err(error) => panic!("unexpected error replacing SDK source: {error:?}"),
    }
    assert_released(&host);
}
