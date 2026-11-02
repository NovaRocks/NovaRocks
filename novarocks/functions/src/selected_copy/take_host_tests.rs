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

//! Symbolic geometry probes. Real host grant/rollback/custody is exercised in
//! Execution, not inferred from these arithmetic-only tests.
use super::*;
use arrow_array::{Int64Array, ListArray, StringArray};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::Field;

#[test]
fn by_copy_joint_invoice_optional_sink_keeps_original_work_and_extent_author() {
    let values: ArrayRef = Arc::new(StringArray::from(vec![
        Some("a"),
        None,
        Some("large selected bytes"),
    ]));
    let source = ListArray::try_new(
        Arc::new(Field::new("item", DataType::Utf8, true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 1, 1, 3])),
        values,
        None,
    )
    .unwrap();
    let indices = [Some(2), None, Some(0), Some(2)];
    let mut original = Vec::new();
    super::super::preflight_take(&source, &indices, |b| {
        original.push(b);
        Ok(())
    })
    .unwrap();
    let mut actual = Vec::new();
    let mut invoice = CopyInvoiceTotals::default();
    super::super::preflight_take_with_invoice(
        &source,
        &indices,
        |b| {
            actual.push(b);
            Ok(())
        },
        |_| Ok(()),
        None,
        ScratchCoverage::RecursiveSelections,
        Some(&mut invoice),
    )
    .unwrap();
    assert_eq!(
        actual, original,
        "no new default/success checkpoint or second value walk"
    );
    let facts = invoice.finish(0).unwrap();
    assert!(facts.operation_peak_bytes() > facts.retained_new_backing_upper());
    assert!(facts.retained_new_backing_upper() >= 2 * "large selected bytes".len());
    assert!(facts.copied_root.is_none());
}
#[test]
fn by_copy_joint_invoice_buffer_growth_covers_real_constructor_hint_and_overlap() {
    for (initial, required) in [
        (0, 0),
        (1, 1),
        (1, 65),
        (1000, 7),
        (64, 129),
        (1024, 100000),
    ] {
        let facts = CopyBufferPeak::mutable(initial, required).unwrap();
        let initial = zip::rounded_capacity(initial).unwrap();
        let mut capacity = initial;
        let mut observed_peak = initial;
        for n in 0..=required {
            if n > capacity {
                let replacement = zip::rounded_capacity(n).unwrap().max(capacity * 2);
                observed_peak = observed_peak.max(capacity + replacement);
                capacity = replacement;
            }
        }
        assert!(facts.retained_upper() >= capacity);
        assert!(facts.transient_upper() >= observed_peak);
    }
}
#[test]
fn by_copy_joint_invoice_result_identity_cannot_be_reused_for_another_copy() {
    let original: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    let another: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    let mut facts = CopyInvoiceTotals::default().finish(0).unwrap();
    assert!(!facts.matches_original_copy(&original));
    facts.bind_original_copy(&original);
    assert!(facts.matches_original_copy(&original));
    assert!(
        !facts.matches_original_copy(&another),
        "equal bytes are not the same allocation/use occurrence"
    );
}

#[test]
fn by_copy_joint_invoice_original_empty_nested_offsets_are_actual_backing() {
    use arrow_array::types::{Int8Type, Int16Type};
    use arrow_array::{DictionaryArray, Int8Array, ListViewArray, RunArray, StructArray};
    let strings: ArrayRef = Arc::new(StringArray::from(vec!["original nonempty"]));
    let item = Arc::new(Field::new("original_item", DataType::Utf8, true));
    let list: ArrayRef = Arc::new(
        ListArray::try_new(
            item.clone(),
            OffsetBuffer::new(vec![0_i32, 1].into()),
            strings.clone(),
            None,
        )
        .unwrap(),
    );
    let structure: ArrayRef = Arc::new(
        StructArray::try_new(
            vec![Arc::new(Field::new(
                "nested",
                list.data_type().clone(),
                true,
            ))]
            .into(),
            vec![list.clone()],
            None,
        )
        .unwrap(),
    );
    let view: ArrayRef = Arc::new(
        ListViewArray::try_new(
            item,
            vec![0_i32].into(),
            vec![1_i32].into(),
            strings.clone(),
            None,
        )
        .unwrap(),
    );
    let dictionary: ArrayRef = Arc::new(
        DictionaryArray::<Int8Type>::try_new(Int8Array::from(vec![0_i8]), strings.clone()).unwrap(),
    );
    let run: ArrayRef = Arc::new(
        RunArray::<Int16Type>::try_new(
            &arrow_array::Int16Array::from(vec![1_i16]),
            strings.as_ref(),
        )
        .unwrap(),
    );
    fn payload(data: &ArrayData) -> usize {
        data.buffers().iter().map(|b| b.capacity()).sum::<usize>()
            + data.nulls().map_or(0, |n| n.buffer().capacity())
            + data.child_data().iter().map(payload).sum::<usize>()
    }
    for source in [strings, list, structure, view, dictionary, run] {
        let indices = UInt32Array::from(Vec::<u32>::new());
        let original = arrow_select::take::take(source.as_ref(), &indices, None).unwrap();
        let mut invoice = CopyInvoiceTotals::default();
        super::super::preflight_take_with_invoice(
            source.as_ref(),
            &[],
            |_| Ok(()),
            |_| Ok(()),
            None,
            ScratchCoverage::RecursiveSelections,
            Some(&mut invoice),
        )
        .unwrap();
        let facts = invoice.finish(0).unwrap();
        assert!(
            facts.retained_new_backing_upper() >= payload(&original.to_data()),
            "empty original carrier {:?} needs its real recursive offset backing",
            source.data_type()
        );
    }
}

#[test]
fn original_filter_null_compression_requests_the_extra_u64_word() {
    for (selected, bytes) in [
        (0, 8),
        (1, 8),
        (63, 8),
        (64, 16),
        (511, 64),
        (512, 72),
        (513, 72),
    ] {
        assert_eq!(original_filter_null_words_bytes(selected).unwrap(), bytes);
    }
}

#[test]
fn original_array_iterator_vec_upper_keeps_minimum_capacity_for_single_slot() {
    assert_eq!(
        original_unreserved_vec_upper::<Option<usize>>(1).unwrap(),
        4 * size_of::<Option<usize>>()
    );
    assert_eq!(
        original_unreserved_vec_upper::<u32>(1).unwrap(),
        4 * size_of::<u32>()
    );
    assert_eq!(
        original_unreserved_vec_upper::<u64>(1).unwrap(),
        4 * size_of::<u64>()
    );
    assert_eq!(original_unreserved_vec_upper::<u64>(0).unwrap(), 0);
}
