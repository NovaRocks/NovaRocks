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

use super::*;
use crate::exec::operators::hashjoin::{
    build_pages::tests::{chunk, work},
    join_completion::tests::{Control, fixture},
};
use arrow::{
    array::{Array, ArrayRef, Int64Array, ListArray, StringViewArray},
    buffer::{OffsetBuffer, ScalarBuffer},
    datatypes::{DataType, Field},
};
use std::sync::Arc;

#[test]
fn payload_compaction_detaches_oversized_slices_and_preserves_signed_values() {
    let (owner, tracker) = fixture();
    let values = Arc::new(Int64Array::from((0..10000).collect::<Vec<i64>>()));
    let original = values.to_data().buffers()[0].data_ptr();
    let source = chunk(Arc::new(values.slice(4000, 3)), false);
    let copied = retain_payload(&source, &work(owner.as_ref(), &tracker, &Control))
        .unwrap()
        .into_chunk();
    let array = copied.columns()[0]
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(array.values().as_ref(), &[4000, 4001, 4002]);
    assert_eq!(array.to_data().buffers()[0].len(), 3 * 8);
    assert_ne!(array.to_data().buffers()[0].data_ptr(), original);
    assert_eq!(
        copied.chunk_schema().slots()[0].typed_binding(),
        source.chunk_schema().slots()[0].typed_binding()
    );
}
#[test]
fn payload_compaction_uses_original_v_for_root_and_nested_view_backing() {
    for nested in [false, true] {
        let (owner, tracker) = fixture();
        let a = "a".repeat(1024);
        let b = "b".repeat(1024);
        let c = "c".repeat(1024);
        let view = Arc::new(StringViewArray::from(vec![
            Some(a.as_str()),
            Some(b.as_str()),
            None,
            Some(c.as_str()),
        ]));
        let old = view.data_buffers()[0].data_ptr();
        let array: ArrayRef = if nested {
            Arc::new(
                ListArray::new(
                    Arc::new(Field::new("item", DataType::Utf8View, true)),
                    OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 1, 4])),
                    view,
                    None,
                )
                .slice(1, 1),
            )
        } else {
            Arc::new(view.slice(1, 3))
        };
        let source = chunk(array, true);
        let copied = retain_payload(&source, &work(owner.as_ref(), &tracker, &Control))
            .unwrap()
            .into_chunk();
        let values = if nested {
            copied.columns()[0]
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .values()
        } else {
            &copied.columns()[0]
        };
        let values = values.as_any().downcast_ref::<StringViewArray>().unwrap();
        assert_eq!(
            values.iter().collect::<Vec<_>>(),
            vec![Some(b.as_str()), None, Some(c.as_str())]
        );
        assert_eq!(
            values
                .data_buffers()
                .iter()
                .map(|buffer| buffer.len())
                .sum::<usize>(),
            2048
        );
        assert!(
            values
                .data_buffers()
                .iter()
                .all(|buffer| buffer.data_ptr() != old)
        );
        assert_eq!(
            copied.chunk_schema().slots()[0].typed_binding(),
            source.chunk_schema().slots()[0].typed_binding()
        );
    }
}
#[test]
fn compacted_payload_last_raw_buffer_keeps_finite_actual_creator() {
    let (owner, tracker) = fixture();
    let array = {
        let source = chunk(
            Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])),
            true,
        );
        let copied = retain_payload(&source, &work(owner.as_ref(), &tracker, &Control))
            .unwrap()
            .into_chunk();
        Arc::clone(&copied.columns()[0])
    };
    drop(owner);
    assert!(tracker.current() > 0);
    let buffer = array.to_data().buffers()[0].clone();
    drop(array);
    assert!(tracker.current() > 0);
    drop(buffer);
    assert_eq!(tracker.current(), 0);
}
#[test]
fn compacted_payload_refuses_before_allocating_and_does_not_change_source() {
    let (owner, tracker) = fixture();
    let source = chunk(
        Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])),
        true,
    );
    let original = source.columns()[0].clone();
    tracker.install_limit_once(tracker.current() + 1).unwrap();
    let cause = retain_payload(&source, &work(owner.as_ref(), &tracker, &Control)).unwrap_err();
    assert!(matches!(
        cause.cause(),
        crate::runtime::task::ExecutionFailureCause::Construction(
            crate::exec::pipeline::operator::ConstructionFailure::Memory(_)
        )
    ));
    assert!(Arc::ptr_eq(&original, &source.columns()[0]));
}
