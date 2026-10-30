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

use super::*;
use allocator_api2::alloc::AllocError;
use arrow_array::{
    BinaryArray, BinaryViewArray, LargeBinaryArray, LargeStringArray, StringArray, StringViewArray,
};
use std::collections::HashMap;
use std::convert::Infallible;
use std::ptr::NonNull;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

const LAYOUTS: [ByteLayout; 6] = [
    ByteLayout::Utf8,
    ByteLayout::LargeUtf8,
    ByteLayout::Utf8View,
    ByteLayout::Binary,
    ByteLayout::LargeBinary,
    ByteLayout::BinaryView,
];

fn bytes(array: &dyn Array) -> Vec<Option<Vec<u8>>> {
    (0..array.len())
        .map(|i| {
            if array.is_null(i) {
                return None;
            }
            macro_rules! string {
                ($ty:ty) => {
                    array
                        .as_any()
                        .downcast_ref::<$ty>()
                        .unwrap()
                        .value(i)
                        .as_bytes()
                        .to_vec()
                };
            }
            macro_rules! binary {
                ($ty:ty) => {
                    array
                        .as_any()
                        .downcast_ref::<$ty>()
                        .unwrap()
                        .value(i)
                        .to_vec()
                };
            }
            Some(
                match ByteLayout::from_data_type(array.data_type()).unwrap() {
                    ByteLayout::Utf8 => string!(StringArray),
                    ByteLayout::LargeUtf8 => string!(LargeStringArray),
                    ByteLayout::Utf8View => string!(StringViewArray),
                    ByteLayout::Binary => binary!(BinaryArray),
                    ByteLayout::LargeBinary => binary!(LargeBinaryArray),
                    ByteLayout::BinaryView => binary!(BinaryViewArray),
                },
            )
        })
        .collect()
}
fn append<A: Allocator + Clone + 'static>(builder: &mut ValueBuilder<A>, value: &[u8]) {
    if builder.layout().is_utf8() {
        builder
            .append_str(
                std::str::from_utf8(value).unwrap(),
                &mut crate::control::global_growth,
                &mut crate::control::uncontrolled,
            )
            .unwrap();
    } else {
        builder
            .append_bytes(
                value,
                &mut crate::control::global_growth,
                &mut crate::control::uncontrolled,
            )
            .unwrap();
    }
}
fn write<E, A: Allocator + Clone + 'static>(
    row: &mut RowWriter<'_, A>,
    layout: ByteLayout,
    value: &[u8],
    growth: &mut Growth<'_, E>,
    work: &mut Work<'_, E>,
) -> Result<(), WriteFailure<E>> {
    if layout.is_utf8() {
        row.write_str(std::str::from_utf8(value).unwrap(), growth, work)
    } else {
        row.write_bytes(value, growth, work)
    }
}
fn complete_row<E, A: Allocator + Clone + 'static>(
    builder: &mut ValueBuilder<A>,
    chunks: &[&[u8]],
    growth: &mut Growth<'_, E>,
    work: &mut Work<'_, E>,
) -> Result<(), WriteFailure<E>> {
    let layout = builder.layout();
    let mut row = builder.begin_row();
    for chunk in chunks {
        write(&mut row, layout, chunk, growth, work)?;
    }
    row.commit(growth, work)
}

#[test]
fn six_frozen_layouts_match_independent_values_nulls_and_fragments() {
    for layout in LAYOUTS {
        let mut builder = ValueBuilder::new(layout);
        assert!(builder.is_empty());
        let values = [
            b"".as_slice(),
            b"123456789012",
            b"1234567890123",
            "你好🌊".as_bytes(),
        ];
        for value in values {
            append(&mut builder, value);
        }
        builder
            .append_null(
                &mut crate::control::global_growth,
                &mut crate::control::uncontrolled,
            )
            .unwrap();
        complete_row(
            &mut builder,
            &[b"multi", b"-", b"part"],
            &mut crate::control::global_growth,
            &mut crate::control::uncontrolled,
        )
        .unwrap();
        let built = builder.finish().unwrap();
        assert_eq!(built.array.data_type(), &layout.data_type());
        let expected = values
            .into_iter()
            .map(|b| Some(b.to_vec()))
            .chain([None, Some(b"multi-part".to_vec())])
            .collect::<Vec<_>>();
        assert_eq!(bytes(built.array.as_ref()), expected);
        assert_eq!(built.facts.rows, 6);
        assert_eq!(
            built.facts.logical_bytes,
            expected
                .iter()
                .flatten()
                .map(|b| b.len() as u128)
                .sum::<u128>()
        );
        assert_eq!(built.facts.logical_bytes, built.facts.copied_bytes);
        built.array.to_data().validate_full().unwrap();
    }
    for layout in [
        ByteLayout::Binary,
        ByteLayout::LargeBinary,
        ByteLayout::BinaryView,
    ] {
        let mut builder = ValueBuilder::new(layout);
        append(&mut builder, &[0xff, 0, 0x80]);
        assert_eq!(
            bytes(builder.finish().unwrap().array.as_ref()),
            [Some(vec![0xff, 0, 0x80])]
        );
    }
}

#[test]
fn empty_outputs_keep_the_exact_frozen_type() {
    for layout in LAYOUTS {
        let built = ValueBuilder::new(layout).finish().unwrap();
        assert_eq!(built.array.data_type(), &layout.data_type());
        assert_eq!(built.facts, BuildFacts::default());
        built.array.to_data().validate_full().unwrap();
        let plan = ValueBuilder::<Global>::planned_layouts(layout, 0, 0, 0).unwrap();
        let mut requests = Vec::new();
        let observed = ValueBuilder::new(layout)
            .finish_observed(
                &mut |layout| {
                    requests.push(layout);
                    Ok::<_, Infallible>(())
                },
                &mut crate::control::uncontrolled,
            )
            .unwrap();
        assert_eq!(requests, plan.iter().copied().collect::<Vec<_>>());
        if !layout.is_view() {
            let expected = if matches!(layout, ByteLayout::Utf8 | ByteLayout::Binary) {
                4
            } else {
                8
            };
            assert_eq!(
                crate::arrow_raw::borrowed_buffer(observed.array.as_ref(), 0)
                    .unwrap()
                    .capacity(),
                expected
            );
        }
        let mut planned = ValueBuilder::new(layout);
        planned
            .reserve_planned_observed(
                0,
                0,
                0,
                &mut crate::control::global_growth,
                &mut crate::control::uncontrolled,
            )
            .unwrap();
        let prepared = planned
            .finish_observed(&mut |_| Err::<(), _>(7), &mut |_| Ok(()))
            .unwrap();
        assert_eq!(prepared.facts, BuildFacts::default());
    }
}

#[test]
fn drop_abort_and_contract_failure_never_publish_partial_rows() {
    for layout in LAYOUTS {
        let mut builder = ValueBuilder::new(layout);
        append(&mut builder, b"committed-prefix");
        let prefix = builder.facts();
        {
            let mut row = builder.begin_row();
            write(
                &mut row,
                layout,
                b"discarded",
                &mut crate::control::global_growth,
                &mut crate::control::uncontrolled,
            )
            .unwrap();
        }
        let mut row = builder.begin_row();
        write(
            &mut row,
            layout,
            b"also-discarded",
            &mut crate::control::global_growth,
            &mut crate::control::uncontrolled,
        )
        .unwrap();
        row.abort();
        let mut row = builder.begin_row();
        write(
            &mut row,
            layout,
            b"failure-discarded",
            &mut crate::control::global_growth,
            &mut crate::control::uncontrolled,
        )
        .unwrap();
        assert!(matches!(
            row.commit_null::<Infallible>(
                &mut crate::control::global_growth,
                &mut crate::control::uncontrolled
            ),
            Err(WriteFailure::Failure(ValueFailure::Value(
                ValueError::Invariant(ValueViolation::RowTransaction)
            )))
        ));
        assert_eq!(builder.len(), 1);
        assert_eq!(builder.facts().copied_bytes, prefix.copied_bytes);
        append(&mut builder, b"retry-once");
        assert_eq!(
            bytes(builder.finish().unwrap().array.as_ref()),
            [
                Some(b"committed-prefix".to_vec()),
                Some(b"retry-once".to_vec())
            ]
        );
    }
}

#[test]
fn growth_and_work_refusal_rollback_every_row_checkpoint_and_retry_once() {
    // The second fragment spans several work checkpoints, so cancellation can
    // occur after real copies rather than only before the first mutation.
    let large = vec![b'x'; COPY_CHECKPOINT * 2 + 1];
    let chunks = [b"head-".as_slice(), large.as_slice(), b"-tail".as_slice()];
    let expected = chunks.concat();
    for layout in LAYOUTS {
        for refuse_work in [false, true] {
            let mut successful = ValueBuilder::new(layout);
            append(&mut successful, b"prefix");
            let mut growth_calls = 0;
            let mut work_calls = 0;
            complete_row(
                &mut successful,
                &chunks,
                &mut |_| {
                    growth_calls += 1;
                    Ok::<_, usize>(())
                },
                &mut |_| {
                    work_calls += 1;
                    Ok(())
                },
            )
            .unwrap();
            let count = if refuse_work {
                work_calls
            } else {
                growth_calls
            };
            assert!(count > 0);
            for refuse_at in 1..=count {
                let mut builder = ValueBuilder::new(layout);
                append(&mut builder, b"prefix");
                let mut n = 0;
                let mut work_n = 0;
                let result = complete_row(
                    &mut builder,
                    &chunks,
                    &mut |_| {
                        n += 1;
                        if !refuse_work && n == refuse_at {
                            Err(77)
                        } else {
                            Ok(())
                        }
                    },
                    &mut |_| {
                        work_n += 1;
                        if refuse_work && work_n == refuse_at {
                            Err(77)
                        } else {
                            Ok(())
                        }
                    },
                );
                assert!(matches!(
                    result,
                    Err(WriteFailure::Failure(ValueFailure::Caller(77)))
                ));
                assert_eq!(builder.len(), 1);
                assert_eq!(builder.facts().logical_bytes, 6);
                assert_eq!(builder.facts().copied_bytes, 6);
                complete_row(
                    &mut builder,
                    &chunks,
                    &mut crate::control::global_growth,
                    &mut crate::control::uncontrolled,
                )
                .unwrap();
                let built = builder.finish().unwrap();
                assert_eq!(
                    bytes(built.array.as_ref()),
                    [Some(b"prefix".to_vec()), Some(expected.clone())]
                );
                built.array.to_data().validate_full().unwrap();
            }
        }
    }
}

#[test]
fn failure_closes_the_writer_and_cannot_commit_an_empty_replacement() {
    let mut builder = ValueBuilder::new(ByteLayout::Utf8);
    let mut row = builder.begin_row();
    assert!(matches!(
        row.write_bytes::<Infallible>(
            b"wrong family",
            &mut crate::control::global_growth,
            &mut crate::control::uncontrolled
        ),
        Err(WriteFailure::Failure(ValueFailure::Value(
            ValueError::Invariant(ValueViolation::Layout)
        )))
    ));
    assert!(matches!(
        row.commit::<Infallible>(
            &mut crate::control::global_growth,
            &mut crate::control::uncontrolled
        ),
        Err(WriteFailure::Failure(ValueFailure::Value(
            ValueError::Invariant(ValueViolation::RowTransaction)
        )))
    ));
    assert!(builder.is_empty());
    append(&mut builder, b"right family");
    assert_eq!(
        bytes(builder.finish().unwrap().array.as_ref()),
        [Some(b"right family".to_vec())]
    );
}

#[test]
fn real_fields_distinguish_full_output_from_unrepresentable_minimum_value() {
    for layout in LAYOUTS {
        let limit = layout.extent_limit();
        check_value_extent::<Infallible>(layout, 0, limit, 0).unwrap();
        let Err(WriteFailure::Failure(ValueFailure::Value(error))) =
            check_value_extent::<Infallible>(layout, 0, limit + 1, 0)
        else {
            panic!("minimum value must fail")
        };
        let facts = error.representation().unwrap();
        assert_eq!(facts.boundary, RepresentationBoundary::MinimumUnit);
        assert_eq!(facts.layout.code.get(), layout as u16);
        assert_eq!(
            (
                facts.required,
                facts.extent_limit,
                facts.representable_prefix
            ),
            (Some(limit + 1), Some(limit), None)
        );
        let result = check_value_extent::<Infallible>(layout, limit - 1, 2, 13);
        if layout.is_view() {
            result.unwrap();
        } else {
            assert!(matches!(result, Err(WriteFailure::Full)));
            // The same row is valid after the owner flushes its prefix.
            check_value_extent::<Infallible>(layout, 0, 2, 0).unwrap();
        }
    }
}

#[test]
fn output_layout_arithmetic_refuses_before_growth_and_preserves_allocator_error() {
    let mut values = AllocVec::<u64, _>::new_in(Global);
    let mut called = false;
    let mut growth = |_| {
        called = true;
        Ok::<_, Infallible>(())
    };
    assert!(matches!(
        reserve(&mut values, usize::MAX, 1, &mut growth),
        Err(WriteFailure::Full)
    ));
    assert!(matches!(
        reserve(&mut values, usize::MAX, 0, &mut growth),
        Err(WriteFailure::Failure(ValueFailure::Value(
            ValueError::Representation(_)
        )))
    ));
    assert!(!called);
    assert!(values.is_empty());
}

#[test]
fn adoption_remaps_slices_without_flushing_the_current_owned_block() {
    for layout in [ByteLayout::Utf8View, ByteLayout::BinaryView] {
        let mut builder = ValueBuilder::new(layout);
        append(&mut builder, b"owned first value");
        let owned_index = builder.current.unwrap();
        let source =
            Buffer::from_vec(b"ignored-abcdefghijklmnop-qrstuvwxyz012345-ignored".to_vec());
        for range in [8..24, 25..41] {
            let visible = source.slice_with_length(range.start, range.len());
            let mut row = builder.begin_row();
            if layout.is_utf8() {
                row.write_string_region(
                    &visible,
                    0..visible.len(),
                    std::str::from_utf8(visible.as_slice()).unwrap(),
                    &mut crate::control::global_growth,
                    &mut crate::control::uncontrolled,
                )
                .unwrap();
            } else {
                row.write_binary_region(
                    &visible,
                    0..visible.len(),
                    &mut crate::control::global_growth,
                    &mut crate::control::uncontrolled,
                )
                .unwrap();
            }
            row.commit(
                &mut crate::control::global_growth,
                &mut crate::control::uncontrolled,
            )
            .unwrap();
        }
        assert_eq!(builder.current, Some(owned_index));
        append(&mut builder, b"owned last value");
        let owned_pointer = match &builder.regions[owned_index] {
            Region::Owned(b) => b.as_ptr(),
            _ => unreachable!(),
        };
        let before_finish = builder.facts();
        assert_eq!(before_finish.adopted_regions, 2);
        assert_eq!(before_finish.copied_bytes, 17 + 16);
        let built = builder.finish().unwrap();
        let buffers = match layout {
            ByteLayout::Utf8View => built
                .array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .unwrap()
                .data_buffers(),
            ByteLayout::BinaryView => built
                .array
                .as_any()
                .downcast_ref::<BinaryViewArray>()
                .unwrap()
                .data_buffers(),
            _ => unreachable!(),
        };
        assert_eq!(buffers[0].as_ptr(), owned_pointer);
        assert_eq!(buffers[1].data_ptr(), source.data_ptr());
        assert_eq!(buffers[2].data_ptr(), source.data_ptr());
        assert_ne!(buffers[1].ptr_offset(), buffers[2].ptr_offset());
        assert_eq!(
            bytes(built.array.as_ref()),
            [
                Some(b"owned first value".to_vec()),
                Some(b"abcdefghijklmnop".to_vec()),
                Some(b"qrstuvwxyz012345".to_vec()),
                Some(b"owned last value".to_vec())
            ]
        );
        built.array.to_data().validate_full().unwrap();
    }
}

#[test]
fn appending_to_an_adopted_fragment_copies_one_complete_row() {
    for layout in [ByteLayout::Utf8View, ByteLayout::BinaryView] {
        let mut builder = ValueBuilder::new(layout);
        let source: ArrayRef = if layout.is_utf8() {
            Arc::new(StringArray::from(vec![Some("one adopted string"), None]))
        } else {
            Arc::new(BinaryArray::from(vec![
                Some(b"one adopted string".as_slice()),
                None,
            ]))
        };
        let mut row = builder.begin_row();
        row.write_array_value(
            source.as_ref(),
            0,
            &mut crate::control::global_growth,
            &mut crate::control::uncontrolled,
        )
        .unwrap();
        write(
            &mut row,
            layout,
            b" + copied tail",
            &mut crate::control::global_growth,
            &mut crate::control::uncontrolled,
        )
        .unwrap();
        row.commit(
            &mut crate::control::global_growth,
            &mut crate::control::uncontrolled,
        )
        .unwrap();
        builder
            .append_array_value(
                source.as_ref(),
                1,
                &mut crate::control::global_growth,
                &mut crate::control::uncontrolled,
            )
            .unwrap();
        let built = builder.finish().unwrap();
        assert_eq!(built.facts.adopted_regions, 0);
        assert_eq!(built.facts.copied_bytes, 18 + 14);
        assert_eq!(
            bytes(built.array.as_ref()),
            [Some(b"one adopted string + copied tail".to_vec()), None]
        );
    }
}

#[derive(Default)]
struct Allocations {
    live: HashMap<usize, Layout>,
    allocations: usize,
    deallocations: usize,
}
#[derive(Clone, Default)]
struct RecordingAllocator {
    state: Arc<Mutex<Allocations>>,
    fail: Arc<AtomicBool>,
}
// SAFETY: all blocks use Global and each clone shares the exact allocation
// record. Successful allocations are recorded once and each matching Layout is
// removed once before Global releases its corresponding block.
unsafe impl Allocator for RecordingAllocator {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        if self.fail.swap(false, Ordering::SeqCst) {
            return Err(AllocError);
        }
        let allocation = Global.allocate(layout)?;
        let mut state = self.state.lock().unwrap();
        assert!(
            state
                .live
                .insert(allocation.as_ptr() as *mut u8 as usize, layout)
                .is_none()
        );
        state.allocations += 1;
        Ok(allocation)
    }
    unsafe fn deallocate(&self, pointer: NonNull<u8>, layout: Layout) {
        let mut state = self.state.lock().unwrap();
        assert_eq!(
            state.live.remove(&(pointer.as_ptr() as usize)),
            Some(layout)
        );
        state.deallocations += 1;
        drop(state);
        // SAFETY: the matching live entry above proves this block came from
        // this allocator's Global instance with exactly the supplied layout.
        unsafe { Global.deallocate(pointer, layout) };
    }
}

#[test]
fn foreign_allocator_blocks_release_with_their_original_allocator_on_finish_and_abort() {
    for layout in LAYOUTS {
        let allocator = RecordingAllocator::default();
        let mut builder = ValueBuilder::new_in(layout, allocator.clone());
        append(&mut builder, b"prefix that stays");
        {
            let mut row = builder.begin_row();
            write(
                &mut row,
                layout,
                b"aborted allocation",
                &mut crate::control::global_growth,
                &mut crate::control::uncontrolled,
            )
            .unwrap();
        }
        append(&mut builder, b"committed after abort");
        assert!(!allocator.state.lock().unwrap().live.is_empty());
        let built = builder.finish().unwrap();
        let state = allocator.state.lock().unwrap();
        assert!(state.allocations > 0);
        assert_eq!(state.allocations, state.deallocations);
        assert!(state.live.is_empty());
        drop(state);
        assert_eq!(
            bytes(built.array.as_ref()),
            [
                Some(b"prefix that stays".to_vec()),
                Some(b"committed after abort".to_vec())
            ]
        );
        drop(built);
        assert!(allocator.state.lock().unwrap().live.is_empty());
    }
}

#[test]
fn allocator_refusal_preserves_the_source_and_allows_exact_retry() {
    for layout in LAYOUTS {
        let allocator = RecordingAllocator::default();
        let mut builder = ValueBuilder::new_in(layout, allocator.clone());
        append(&mut builder, b"prefix");
        let large = vec![b'a'; BLOCK_TARGET * 2];
        allocator.fail.store(true, Ordering::SeqCst);
        let mut row = builder.begin_row();
        let error = write(
            &mut row,
            layout,
            &large,
            &mut crate::control::global_growth,
            &mut crate::control::uncontrolled,
        )
        .unwrap_err();
        let WriteFailure::Failure(ValueFailure::Value(error)) = error else {
            panic!("allocator error")
        };
        assert!(matches!(error, ValueError::Allocator { .. }));
        assert!(
            std::error::Error::source(&error)
                .unwrap()
                .is::<allocator_api2::collections::TryReserveError>()
        );
        drop(row);
        append(&mut builder, &large);
        assert_eq!(
            bytes(builder.finish().unwrap().array.as_ref()),
            [Some(b"prefix".to_vec()), Some(large)]
        );
        assert!(allocator.state.lock().unwrap().live.is_empty());
    }
}

#[test]
fn global_outputs_retain_standard_arrow_buffer_reuse_without_copy() {
    for layout in LAYOUTS {
        let mut builder = ValueBuilder::new(layout);
        append(&mut builder, b"a payload that is longer than inline");
        let pointer = if layout.is_view() {
            let Region::Owned(bytes) = &builder.regions[builder.current.unwrap()] else {
                unreachable!()
            };
            bytes.as_ptr()
        } else {
            builder.payload.as_ptr()
        };
        let built = builder.finish().unwrap();
        macro_rules! offsets {
            ($ty:ty) => {{
                let array = built.array.as_any().downcast_ref::<$ty>().unwrap().clone();
                drop(built);
                let (_, payload, _) = array.into_parts();
                payload
            }};
        }
        macro_rules! view {
            ($ty:ty) => {{
                let array = built.array.as_any().downcast_ref::<$ty>().unwrap().clone();
                drop(built);
                let (_, regions, _) = array.into_parts();
                let payload = regions[0].clone();
                drop(regions);
                payload
            }};
        }
        let payload = match layout {
            ByteLayout::Utf8 => offsets!(StringArray),
            ByteLayout::LargeUtf8 => offsets!(LargeStringArray),
            ByteLayout::Binary => offsets!(BinaryArray),
            ByteLayout::LargeBinary => offsets!(LargeBinaryArray),
            ByteLayout::Utf8View => view!(StringViewArray),
            ByteLayout::BinaryView => view!(BinaryViewArray),
        };
        assert_eq!(payload.as_ptr(), pointer);
        assert!(payload.into_mutable().is_ok());
    }
}

struct SourceOwner {
    bytes: Vec<u8>,
    drops: Arc<AtomicUsize>,
}
impl Drop for SourceOwner {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
fn tracked_buffer(bytes: &[u8], drops: Arc<AtomicUsize>) -> Buffer {
    let owner = Arc::new(SourceOwner {
        bytes: bytes.to_vec(),
        drops,
    });
    let pointer = NonNull::new(owner.bytes.as_ptr() as *mut u8).unwrap();
    // SAFETY: the immutable Vec remains at this address until the final Buffer
    // releases the owner; only the initialized Vec length is exposed.
    unsafe { Buffer::from_custom_allocation(pointer, owner.bytes.len(), owner) }
}

#[test]
fn adopted_owner_survives_output_clone_slice_and_unpublished_adoption_exits() {
    for layout in [ByteLayout::Utf8View, ByteLayout::BinaryView] {
        let drops = Arc::new(AtomicUsize::new(0));
        let buffer = tracked_buffer(b"source retained by its real owner", drops.clone());
        let mut builder = ValueBuilder::new(layout);
        let mut row = builder.begin_row();
        if layout.is_utf8() {
            row.write_string_region(
                &buffer,
                0..buffer.len(),
                std::str::from_utf8(buffer.as_slice()).unwrap(),
                &mut crate::control::global_growth,
                &mut crate::control::uncontrolled,
            )
            .unwrap();
        } else {
            row.write_binary_region(
                &buffer,
                0..buffer.len(),
                &mut crate::control::global_growth,
                &mut crate::control::uncontrolled,
            )
            .unwrap();
        }
        let failed = row.commit(&mut |_| Err::<(), _>(91), &mut |_| Ok(()));
        assert!(matches!(
            failed,
            Err(WriteFailure::Failure(ValueFailure::Caller(91)))
        ));
        assert_eq!(builder.facts().adopted_regions, 0);
        let mut row = builder.begin_row();
        if layout.is_utf8() {
            row.write_string_region(
                &buffer,
                0..buffer.len(),
                std::str::from_utf8(buffer.as_slice()).unwrap(),
                &mut crate::control::global_growth,
                &mut crate::control::uncontrolled,
            )
            .unwrap();
        } else {
            row.write_binary_region(
                &buffer,
                0..buffer.len(),
                &mut crate::control::global_growth,
                &mut crate::control::uncontrolled,
            )
            .unwrap();
        }
        row.commit(
            &mut crate::control::global_growth,
            &mut crate::control::uncontrolled,
        )
        .unwrap();
        let built = builder.finish().unwrap();
        drop(buffer);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        let alias = built.array.clone();
        let slice = alias.slice(0, 1);
        drop(built);
        drop(alias);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert_eq!(
            bytes(slice.as_ref()),
            [Some(b"source retained by its real owner".to_vec())]
        );
        drop(slice);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn real_64_mib_and_64_mib_plus_one_values_have_no_internal_quota() {
    // One test owns this input and releases each output before the next layout;
    // there are no simultaneously running copies of the large-value matrix.
    let input = vec![b'x'; 64 * 1024 * 1024 + 1];
    for len in [64 * 1024 * 1024, 64 * 1024 * 1024 + 1] {
        for layout in LAYOUTS {
            let mut builder = ValueBuilder::new(layout);
            append(&mut builder, &input[..len]);
            let built = builder.finish().unwrap();
            assert_eq!(built.array.len(), 1);
            assert_eq!(built.facts.logical_bytes, len as u128);
            assert_eq!(built.facts.copied_bytes, len as u128);
            assert_eq!(
                crate::arrow_raw::byte_span(built.array.as_ref(), 0)
                    .unwrap()
                    .unwrap()
                    .bytes(),
                &input[..len]
            );
            built.array.to_data().validate_full().unwrap();
            drop(built);
        }
    }
}

#[test]
fn string_adoption_requires_the_proven_utf8_range_and_null_is_not_a_fragment() {
    let source = Buffer::from_vec("hello-你好-world".as_bytes().to_vec());
    let mut builder = ValueBuilder::new(ByteLayout::Utf8View);
    append(&mut builder, b"prefix");
    let mut row = builder.begin_row();
    let different_storage = "hello-你好-world".to_owned();
    let error = row
        .write_string_region::<Infallible>(
            &source,
            0..source.len(),
            &different_storage,
            &mut crate::control::global_growth,
            &mut crate::control::uncontrolled,
        )
        .unwrap_err();
    assert!(matches!(
        error,
        WriteFailure::Failure(ValueFailure::Value(ValueError::Invariant(
            ValueViolation::Utf8Boundary
        )))
    ));
    drop(row);
    let text = std::str::from_utf8(source.as_slice()).unwrap();
    let mut row = builder.begin_row();
    row.write_string_region(
        &source,
        6..12,
        &text[6..12],
        &mut crate::control::global_growth,
        &mut crate::control::uncontrolled,
    )
    .unwrap();
    row.commit(
        &mut crate::control::global_growth,
        &mut crate::control::uncontrolled,
    )
    .unwrap();
    let null = StringArray::from(vec![None::<&str>]);
    let mut row = builder.begin_row();
    row.write_array_value(
        &null,
        0,
        &mut crate::control::global_growth,
        &mut crate::control::uncontrolled,
    )
    .unwrap();
    assert!(matches!(
        row.write_str::<Infallible>(
            "illegal tail",
            &mut crate::control::global_growth,
            &mut crate::control::uncontrolled
        ),
        Err(WriteFailure::Failure(ValueFailure::Value(
            ValueError::Invariant(ValueViolation::RowTransaction)
        )))
    ));
    drop(row);
    builder
        .begin_row()
        .commit(
            &mut crate::control::global_growth,
            &mut crate::control::uncontrolled,
        )
        .unwrap();
    assert_eq!(
        bytes(builder.finish().unwrap().array.as_ref()),
        [
            Some(b"prefix".to_vec()),
            Some("你好".as_bytes().to_vec()),
            Some(vec![])
        ]
    );
}

#[test]
fn logical_volume_of_shared_values_crosses_u64_without_an_output_boundary() {
    // Descriptor count and one legal view value define this arithmetic. No
    // product limit or synthetic shortened field is introduced for the test.
    let row_bytes = i32::MAX as usize;
    let rows_before_crossing = u64::MAX as u128 / row_bytes as u128;
    let before = rows_before_crossing * row_bytes as u128;
    let after = add_byte_volume(before, row_bytes);
    assert!(before <= u64::MAX as u128);
    assert!(after > u64::MAX as u128);
    assert_eq!(after, (rows_before_crossing + 1) * row_bytes as u128);
    let maximum_rows = usize::MAX as u128;
    let maximum_row_bytes = (i64::MAX as u128).min(usize::MAX as u128);
    assert_eq!(
        add_byte_volume(
            (maximum_rows - 1) * maximum_row_bytes,
            maximum_row_bytes as usize
        ),
        maximum_rows * maximum_row_bytes
    );
}

fn foreign_publication_builder(
    layout: ByteLayout,
    allocator: RecordingAllocator,
) -> ValueBuilder<RecordingAllocator> {
    let mut builder = ValueBuilder::new_in(layout, allocator);
    for value in [b"1234567890123".as_slice(), b"1234567890123", b"x"] {
        append(&mut builder, value);
    }
    builder
        .append_null(
            &mut crate::control::global_growth,
            &mut crate::control::uncontrolled,
        )
        .unwrap();
    builder
}

#[test]
fn foreign_publication_reports_copies_and_real_output_capacity() {
    for layout in LAYOUTS {
        let allocator = RecordingAllocator::default();
        let builder = foreign_publication_builder(layout, allocator.clone());
        assert!(builder.facts().declared_payload_capacity > 27);
        let mut growth = Vec::new();
        let mut observed_work = 0;
        let built = builder
            .finish_observed(
                &mut |request| {
                    growth.push(request);
                    Ok::<_, Infallible>(())
                },
                &mut |n| {
                    observed_work += n;
                    Ok(())
                },
            )
            .unwrap();
        assert_eq!(built.facts.logical_bytes, 27);
        assert_eq!(built.facts.copied_bytes, 27);
        let metadata_bytes = if layout.is_view() {
            4 * 16
        } else if matches!(layout, ByteLayout::Utf8 | ByteLayout::Binary) {
            5 * 4
        } else {
            5 * 8
        };
        assert_eq!(
            built.facts.publication_copied_bytes,
            27 + metadata_bytes + 1
        );
        assert_eq!(built.facts.declared_payload_capacity, 27);
        assert!(growth.iter().any(|layout| layout.size() == 27));
        assert!(
            growth
                .iter()
                .any(|layout| layout.size() == metadata_bytes as usize)
        );
        assert!(growth.iter().any(|layout| layout.size() == 1));
        assert!(observed_work >= built.facts.publication_copied_bytes as usize);
        assert!(allocator.state.lock().unwrap().live.is_empty());
        built.array.to_data().validate_full().unwrap();
    }
}

#[test]
fn publication_growth_or_cancellation_preserves_caller_and_releases_unpublished_storage() {
    for layout in LAYOUTS {
        let mut growth_calls = 0;
        let mut work_calls = 0;
        foreign_publication_builder(layout, RecordingAllocator::default())
            .finish_observed(
                &mut |_| {
                    growth_calls += 1;
                    Ok::<_, Infallible>(())
                },
                &mut |_| {
                    work_calls += 1;
                    Ok(())
                },
            )
            .unwrap();
        for refuse_work in [false, true] {
            let count = if refuse_work {
                work_calls
            } else {
                growth_calls
            };
            assert!(count > 0);
            for refuse_at in 1..=count {
                let allocator = RecordingAllocator::default();
                let builder = foreign_publication_builder(layout, allocator.clone());
                let cause = Arc::new(53);
                let mut growth_n = 0;
                let mut work_n = 0;
                let result = builder.finish_observed(
                    &mut |_| {
                        growth_n += 1;
                        if !refuse_work && growth_n == refuse_at {
                            Err(cause.clone())
                        } else {
                            Ok(())
                        }
                    },
                    &mut |_| {
                        work_n += 1;
                        if refuse_work && work_n == refuse_at {
                            Err(cause.clone())
                        } else {
                            Ok(())
                        }
                    },
                );
                let Err(ValueFailure::Caller(original)) = result else {
                    panic!("original publication refusal")
                };
                assert!(Arc::ptr_eq(&cause, &original));
                let state = allocator.state.lock().unwrap();
                assert!(state.live.is_empty());
                assert_eq!(state.allocations, state.deallocations);
            }
        }
    }
}

#[test]
fn global_publication_transfers_buffers_and_only_observes_the_new_region_table() {
    for layout in LAYOUTS {
        let mut builder = ValueBuilder::new(layout);
        append(&mut builder, b"one owned non-inline value");
        let regions = builder.regions.len();
        let mut requests = Vec::new();
        let mut work = 0;
        let built = builder
            .finish_observed(
                &mut |layout| {
                    requests.push(layout);
                    Ok::<_, Infallible>(())
                },
                &mut |n| {
                    work += n;
                    Ok(())
                },
            )
            .unwrap();
        assert_eq!(built.facts.publication_copied_bytes, 0);
        if layout.is_view() {
            assert_eq!(requests, [Layout::array::<Buffer>(regions).unwrap()]);
            assert_eq!(work, regions);
        } else {
            assert!(requests.is_empty());
            assert_eq!(work, 0);
        }
    }
}

#[test]
fn empty_publication_drops_aborted_capacity_and_reports_empty_storage() {
    for layout in LAYOUTS {
        let allocator = RecordingAllocator::default();
        let mut builder = ValueBuilder::new_in(layout, allocator.clone());
        let mut row = builder.begin_row();
        write(
            &mut row,
            layout,
            b"an uncommitted allocation with capacity",
            &mut crate::control::global_growth,
            &mut crate::control::uncontrolled,
        )
        .unwrap();
        row.abort();
        let mut requests = Vec::new();
        let built = builder
            .finish_observed(
                &mut |layout| {
                    requests.push(layout);
                    Ok::<_, Infallible>(())
                },
                &mut crate::control::uncontrolled,
            )
            .unwrap();
        let sentinel_bytes = if layout.is_view() {
            0
        } else if matches!(layout, ByteLayout::Utf8 | ByteLayout::Binary) {
            4
        } else {
            8
        };
        assert_eq!(
            built.facts,
            BuildFacts {
                publication_copied_bytes: sentinel_bytes,
                ..BuildFacts::default()
            }
        );
        assert_eq!(
            requests.iter().map(Layout::size).collect::<Vec<_>>(),
            if layout.is_view() {
                vec![]
            } else {
                vec![sentinel_bytes as usize; 2]
            }
        );
        assert!(allocator.state.lock().unwrap().live.is_empty());
        built.array.to_data().validate_full().unwrap();
        assert!(matches!(
            ValueBuilder::new_in(layout, allocator)
                .finish_observed(&mut |_| Ok(()), &mut |_| Err(7)),
            Err(ValueFailure::Caller(7))
        ));
    }
}

fn conversion_source(layout: ByteLayout, values: &[Option<&str>]) -> ArrayRef {
    if layout.is_utf8() {
        Arc::new(StringArray::from(values.to_vec()))
    } else {
        Arc::new(BinaryArray::from(
            values
                .iter()
                .map(|v| v.map(str::as_bytes))
                .collect::<Vec<_>>(),
        ))
    }
}

#[test]
fn planned_capacity_requests_match_actual_reservations_and_appends_do_not_grow() {
    let long = "x".repeat(58);
    let mut values = vec![Some(""); 128];
    values[0] = Some(&long);
    values[17] = None;
    for layout in LAYOUTS {
        let payload = if layout.is_view() { 0 } else { 58 };
        let adopted = usize::from(layout.is_view());
        let plan = ValueBuilder::<Global>::planned_layouts(layout, values.len(), payload, adopted)
            .unwrap();
        assert_eq!(plan.len(), plan.iter().count());
        let source = conversion_source(layout, &values);
        let mut builder = ValueBuilder::new(layout);
        let mut actual = Vec::new();
        builder
            .reserve_planned_observed(
                values.len(),
                payload,
                adopted,
                &mut |layout| {
                    actual.push(layout);
                    Ok::<_, Infallible>(())
                },
                &mut crate::control::uncontrolled,
            )
            .unwrap();
        assert_eq!(actual, plan.iter().copied().collect::<Vec<_>>());
        assert_eq!(plan.clone().into_iter().collect::<Vec<_>>(), actual);
        if layout.is_view() {
            assert_eq!(builder.views.capacity(), 128);
            assert_eq!(builder.regions.capacity(), 2);
            assert_eq!(builder.publication_regions.as_ref().unwrap().capacity(), 2);
        } else {
            assert_eq!(builder.payload.capacity(), 58);
            assert_eq!(
                if matches!(layout, ByteLayout::Utf8 | ByteLayout::Binary) {
                    builder.offsets32.capacity()
                } else {
                    builder.offsets64.capacity()
                },
                129
            );
        }
        assert_eq!(builder.validity.capacity(), 16);
        for row in 0..values.len() {
            builder
                .append_array_value(source.as_ref(), row, &mut |_| Err::<(), _>(99), &mut |_| {
                    Ok(())
                })
                .unwrap();
        }
        let built = builder
            .finish_observed(&mut |_| Err::<(), _>(99), &mut |_| Ok(()))
            .unwrap();
        assert_eq!(built.facts.logical_bytes, 58);
        assert_eq!(
            bytes(built.array.as_ref()),
            values
                .iter()
                .map(|v| v.map(|s| s.as_bytes().to_vec()))
                .collect::<Vec<_>>()
        );
        built.array.to_data().validate_full().unwrap();
    }
}

#[test]
fn planned_inline_region_crosses_the_block_target_without_hidden_growth() {
    let values = vec![Some("123456789012"); BLOCK_TARGET / 12 + 2];
    let inline_bytes = values.len() * 12;
    assert!(inline_bytes > BLOCK_TARGET);
    for layout in [ByteLayout::Utf8View, ByteLayout::BinaryView] {
        let source = conversion_source(layout, &values);
        let mut builder = ValueBuilder::new(layout);
        builder
            .reserve_planned_observed(
                values.len(),
                inline_bytes,
                0,
                &mut crate::control::global_growth,
                &mut crate::control::uncontrolled,
            )
            .unwrap();
        for row in 0..values.len() {
            builder
                .append_array_value(source.as_ref(), row, &mut |_| Err::<(), _>(99), &mut |_| {
                    Ok(())
                })
                .unwrap();
        }
        assert_eq!(builder.regions.len(), 1);
        let built = builder
            .finish_observed(&mut |_| Err::<(), _>(99), &mut |_| Ok(()))
            .unwrap();
        assert_eq!(built.facts.logical_bytes, inline_bytes as u128);
        assert_eq!(built.facts.declared_payload_capacity, inline_bytes as u128);
        assert_eq!(built.facts.adopted_regions, 0);
        assert!(
            bytes(built.array.as_ref())
                .iter()
                .all(|v| v.as_deref() == Some(b"123456789012".as_slice()))
        );
        built.array.to_data().validate_full().unwrap();
    }
}

#[test]
fn planned_reservation_refusal_is_atomic_and_the_same_builder_can_retry() {
    for layout in LAYOUTS {
        let plan = ValueBuilder::<RecordingAllocator>::planned_layouts(layout, 8, 12, 0).unwrap();
        for refuse_work in [false, true] {
            let count = plan.len() + usize::from(refuse_work);
            for refuse_at in 1..=count {
                let allocator = RecordingAllocator::default();
                let mut builder = ValueBuilder::new_in(layout, allocator.clone());
                let original = Arc::new(82);
                let mut growth_n = 0;
                let mut work_n = 0;
                let result = builder.reserve_planned_observed(
                    8,
                    12,
                    0,
                    &mut |_| {
                        growth_n += 1;
                        if !refuse_work && growth_n == refuse_at {
                            Err(original.clone())
                        } else {
                            Ok(())
                        }
                    },
                    &mut |_| {
                        work_n += 1;
                        if refuse_work && work_n == refuse_at {
                            Err(original.clone())
                        } else {
                            Ok(())
                        }
                    },
                );
                let Err(ValueFailure::Caller(cause)) = result else {
                    panic!("planned storage refusal")
                };
                assert!(Arc::ptr_eq(&cause, &original));
                assert_eq!(builder.facts(), BuildFacts::default());
                assert_eq!(builder.payload.capacity(), 0);
                assert_eq!(builder.regions.capacity(), 0);
                assert!(builder.publication_regions.is_none());
                assert!(allocator.state.lock().unwrap().live.is_empty());
                builder
                    .reserve_planned_observed(
                        8,
                        12,
                        0,
                        &mut crate::control::global_growth,
                        &mut crate::control::uncontrolled,
                    )
                    .unwrap();
                let error = builder
                    .reserve_planned_observed(
                        8,
                        12,
                        0,
                        &mut |_| panic!("prepared storage must not be requested again"),
                        &mut |_| Ok::<_, Infallible>(()),
                    )
                    .unwrap_err();
                assert!(matches!(
                    error,
                    ValueFailure::Value(ValueError::Invariant(ValueViolation::RowTransaction))
                ));
                let source = conversion_source(layout, &[Some("123456789012")]);
                builder
                    .append_array_value(source.as_ref(), 0, &mut |_| Err::<(), _>(99), &mut |_| {
                        Ok(())
                    })
                    .unwrap();
                let built = builder.finish().unwrap();
                assert_eq!(
                    bytes(built.array.as_ref()),
                    [Some(b"123456789012".to_vec())]
                );
                assert!(allocator.state.lock().unwrap().live.is_empty());
            }
        }
    }
}

#[test]
fn planned_arithmetic_preserves_single_output_and_minimum_value_units() {
    let required = i32::MAX as usize + 1;
    for (rows, boundary) in [
        (1, RepresentationBoundary::MinimumUnit),
        (2, RepresentationBoundary::SingleOutput),
    ] {
        let error = ValueBuilder::<Global>::planned_layouts(ByteLayout::Utf8, rows, required, 0)
            .unwrap_err();
        let facts = error.representation().unwrap();
        assert_eq!(facts.boundary, boundary);
        assert_eq!(facts.layout.code.get(), ByteLayout::Utf8 as u16);
        assert_eq!(
            (
                facts.required,
                facts.extent_limit,
                facts.representable_prefix
            ),
            (Some(required as u64), Some(i32::MAX as u64), None)
        );
    }
    let error = ValueBuilder::<Global>::planned_layouts(ByteLayout::Utf8View, usize::MAX, 0, 0)
        .unwrap_err();
    let facts = error.representation().unwrap();
    assert_eq!(facts.boundary, RepresentationBoundary::SingleOutput);
    assert_eq!(facts.representable_prefix, None);
    let empty = ValueBuilder::<Global>::planned_layouts(ByteLayout::Utf8View, 0, 0, 0).unwrap();
    assert!(empty.is_empty());
}

#[test]
fn view_table_structure_refusal_precedes_publication_and_keeps_original_sources() {
    use crate::control::ValueStructure;
    for layout in [ByteLayout::Utf8View, ByteLayout::BinaryView] {
        for count in [0, 1, 64] {
            let drops = Arc::new(AtomicUsize::new(0));
            let sources = (0..count)
                .map(|_| tracked_buffer(b"independent long shared region", drops.clone()))
                .collect::<Vec<_>>();
            let mut builder = ValueBuilder::new(layout);
            for source in &sources {
                let mut row = builder.begin_row();
                if layout.is_utf8() {
                    row.write_string_region(
                        source,
                        0..source.len(),
                        std::str::from_utf8(source.as_slice()).unwrap(),
                        &mut crate::control::global_growth,
                        &mut crate::control::uncontrolled,
                    )
                    .unwrap();
                } else {
                    row.write_binary_region(
                        source,
                        0..source.len(),
                        &mut crate::control::global_growth,
                        &mut crate::control::uncontrolled,
                    )
                    .unwrap();
                }
                row.commit(
                    &mut crate::control::global_growth,
                    &mut crate::control::uncontrolled,
                )
                .unwrap();
            }
            let expected = novarocks_type_contract::owned_resources::layout::arc_layout(
                Layout::array::<Buffer>(count).unwrap(),
            )
            .unwrap();
            let cause = Arc::new(71);
            let mut byte_arrays = 0;
            let mut shared_tables = 0;
            let result = builder.finish_structured_observed(
                &mut |_| -> Result<(), Arc<i32>> { panic!("growth after table refusal") },
                &mut |_| -> Result<(), Arc<i32>> { panic!("work after table refusal") },
                &mut |event| match event {
                    ValueStructure::ByteArray { .. } => {
                        byte_arrays += 1;
                        Ok(())
                    }
                    ValueStructure::SharedOwner(actual) => {
                        shared_tables += 1;
                        assert_eq!(actual, expected);
                        Err(cause.clone())
                    }
                },
            );
            let Err(ValueFailure::Caller(actual)) = result else {
                panic!("original shared-table observer refusal")
            };
            assert!(Arc::ptr_eq(&actual, &cause));
            assert_eq!((byte_arrays, shared_tables), (1, 1));
            assert_eq!(drops.load(Ordering::SeqCst), 0);
            for source in &sources {
                assert_eq!(source.as_slice(), b"independent long shared region");
            }
            drop(sources);
            assert_eq!(drops.load(Ordering::SeqCst), count);
        }
    }
}
