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
use crate::ipc_flat_stream_v2::progress::IpcReaderProgressFacts;
use crate::ipc_recursive_stream_v2::{
    RecursiveBatchProjectionLimits, RecursiveReaderProjectionLimits,
    RecursiveStreamProjectionLimits, preflight_recursive_constant_stream_in,
};
use arrow::array::StructArray;
use std::alloc::Layout;

fn cause(result: &Result<ConstantPool, FlatReaderError>) -> Option<CompileControlError> {
    match result {
        Err(FlatReaderError::Projection(FlatPoolResourceError::Control(c))) => Some(c.clone()),
        _ => None,
    }
}
fn finish(
    work: CompileCheckpoints<'_>,
    result: Result<ConstantPool, FlatReaderError>,
) -> Result<ConstantPool, FlatReaderError> {
    if cause(&result).is_some() {
        return result;
    }
    work.finish()?;
    result
}
fn flat_run(
    input: &[u8],
    field: &Arc<Field>,
    ty: &FunctionValueType,
    source: usize,
    control: &dyn PureCompileControl,
    admit: &mut impl FnMut(&IpcReaderProgressFacts) -> Result<(), CompileControlError>,
) -> Result<ConstantPool, FlatReaderError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = (|| {
        let stream = preflight_flat_constant_stream_in(
            input,
            field,
            stream_limits(),
            &verifier(),
            source,
            admit,
            &mut work,
        )
        .map_err(FlatPoolResourceError::from)?;
        let prepared = stream.prepare_pool_borrowed_in(
            Arc::clone(field),
            ty,
            source,
            policy(),
            reader_limits(),
            admit,
            &mut work,
        )?;
        let complete = *prepared.facts();
        let snapshot = IpcReaderProgressFacts {
            source_retained_bytes: complete.source_retained_bytes,
            allocation_request_count_upper_bound: complete.allocation_request_count_upper_bound,
            new_allocation_request_bytes_upper_bound: complete
                .new_allocation_request_bytes_upper_bound,
            cumulative_library_work_upper_bound: complete.cumulative_library_work_upper_bound,
            geometry_scratch_request_bytes: 0,
            geometry_scratch_request_count: 0,
        };
        admit(&snapshot)?;
        let mut consume = |f: &IpcReaderProgressFacts| {
            assert!(
                f.new_allocation_request_bytes_upper_bound
                    <= complete.new_allocation_request_bytes_upper_bound
            );
            assert!(
                f.allocation_request_count_upper_bound
                    <= complete.allocation_request_count_upper_bound
            );
            assert!(
                f.cumulative_library_work_upper_bound
                    <= complete.cumulative_library_work_upper_bound
            );
            admit(f)
        };
        prepared.materialize_in(&mut consume, &mut work)
    })();
    finish(work, result)
}
fn recursive_limits() -> RecursiveStreamProjectionLimits {
    RecursiveStreamProjectionLimits {
        max_input_bytes: 1 << 24,

        batch: RecursiveBatchProjectionLimits {
            flat: FlatBatchProjectionLimits {
                max_metadata_bytes: 1 << 20,
                max_body_bytes: 1 << 24,
                max_rows: 4096,
                max_buffer_descriptors: 16384,
                max_view_validation_bytes: 1 << 24,
            },
            max_field_nodes: 4096,
            max_total_rows: 1 << 20,
            max_geometry_request_bytes: 1 << 20,
        },
    }
}
fn recursive_policy() -> ConstantPolicy {
    ConstantPolicy {
        max_array_nodes: 4096,
        max_type_nodes: 4096,
        max_logical_elements: 1 << 20,
        max_retained_buffer_bytes: 1 << 24,
        max_library_validation_work: 1 << 28,
        max_library_validation_bytes: 1 << 28,
        ..policy()
    }
}
fn recursive_run(
    input: &[u8],
    field: &Arc<Field>,
    ty: &FunctionValueType,
    source: usize,
    control: &dyn PureCompileControl,
    admit: &mut impl FnMut(&IpcReaderProgressFacts) -> Result<(), CompileControlError>,
) -> Result<ConstantPool, FlatReaderError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = (|| {
        let stream = preflight_recursive_constant_stream_in(
            input,
            field,
            recursive_limits(),
            &verifier(),
            source,
            admit,
            &mut work,
        )
        .map_err(FlatPoolResourceError::from)?;
        let prepared = stream.prepare_pool_borrowed_in(
            Arc::clone(field),
            ty,
            source,
            recursive_policy(),
            RecursiveReaderProjectionLimits {
                max_new_allocation_request_bytes: 1 << 28,
                max_coexisting_source_and_request_bytes: 1 << 29,
                max_cumulative_library_work: 4_000_000_000,
            },
            admit,
            &mut work,
        )?;
        // Independent Layout oracle: root Struct plus its two real children.
        let complete = *prepared.facts();
        let geometry = prepared.geometry_scratch_request_bytes();
        assert_eq!(prepared.geometry_scratch_request_count(), 1);
        assert!(geometry > 0);
        assert!(complete.new_allocation_request_bytes_upper_bound >= geometry);
        let mut consume = |f: &IpcReaderProgressFacts| {
            assert!(
                f.new_allocation_request_bytes_upper_bound
                    <= complete.new_allocation_request_bytes_upper_bound
            );
            assert!(
                f.allocation_request_count_upper_bound
                    <= complete.allocation_request_count_upper_bound
            );
            assert!(
                f.cumulative_library_work_upper_bound
                    <= complete.cumulative_library_work_upper_bound
            );
            admit(f)
        };
        prepared.materialize_in(&mut consume, &mut work)
    })();
    finish(work, result)
}
fn scalar_fixture() -> (Vec<u8>, Arc<Field>, FunctionValueType, usize) {
    let field = Arc::new(Field::new("value", DataType::Int64, true));
    let array: ArrayRef = Arc::new(Int64Array::from(vec![Some(41), None, Some(-3)]));
    let input = writer_stream(array, &field);
    let source = retained(&input, &field);
    let ty = FunctionValueType::new(DataType::Int64, true);
    (input, field, ty, source)
}
fn nested_fixture() -> (Vec<u8>, Arc<Field>, FunctionValueType, usize) {
    let fields = vec![
        Arc::new(Field::new("left", DataType::Int64, true)),
        Arc::new(Field::new("right", DataType::Int64, true)),
    ];
    let array: ArrayRef = Arc::new(StructArray::new(
        fields.into(),
        vec![
            Arc::new(Int64Array::from(vec![Some(5), None])),
            Arc::new(Int64Array::from(vec![Some(8), Some(9)])),
        ],
        None,
    ));
    let field = Arc::new(Field::new("pair", array.data_type().clone(), true));
    let input = writer_stream(array, &field);
    let source = input.capacity() + (1 << 16);
    let ty = FunctionValueType::try_from_field(&field).unwrap();
    (input, field, ty, source)
}
fn prefixes(
    baseline: &[(CompilePhase, u32)],
    mut run: impl FnMut(&Control) -> Result<ConstantPool, FlatReaderError>,
) {
    assert!(!baseline.is_empty());
    for at in 0..baseline.len() {
        for c in CAUSES {
            let control = Control::refusing(at, c.clone());
            assert_eq!(cause(&run(&control)), Some(c));
            assert_eq!(control.trace(), baseline[..=at]);
        }
    }
}

#[test]
fn flat_caller_scope_keeps_actual_values_and_every_success_callback() {
    let (input, field, ty, source) = scalar_fixture();
    let control = Control::good();
    let pool = flat_run(&input, &field, &ty, source, &control, &mut |_| Ok(())).unwrap();
    assert!(std::ptr::eq(pool.field(), field.as_ref()));
    assert_eq!(pool.value_type(), &ty);
    assert_eq!(pool.value(0).unwrap().try_i64().unwrap(), Some(41));
    assert_eq!(pool.value(1).unwrap().try_i64().unwrap(), None);
    assert_eq!(pool.value(2).unwrap().try_i64().unwrap(), Some(-3));
    prefixes(&control.trace(), |c| {
        flat_run(&input, &field, &ty, source, c, &mut |_| Ok(()))
    });
}

#[test]
fn recursive_caller_scope_preserves_children_geometry_and_every_callback() {
    let (input, field, ty, source) = nested_fixture();
    let control = Control::good();
    let mut captures = Vec::new();
    let pool = recursive_run(&input, &field, &ty, source, &control, &mut |f| {
        captures.push(*f);
        Ok(())
    })
    .unwrap();
    assert_eq!(pool.value_type(), &ty);
    assert!(std::ptr::eq(pool.field(), field.as_ref()));
    let data = pool.data();
    assert_eq!(data.len(), 2);
    assert_eq!(data.child_data().len(), 2);
    let expected = Layout::array::<crate::ipc_recursive_batch_v2::RecursiveNodeGeometry<'_>>(3)
        .unwrap()
        .size();
    assert!(
        captures
            .iter()
            .any(|f| f.geometry_scratch_request_bytes == expected
                && f.geometry_scratch_request_count == 1)
    );
    let complete = *captures.last().unwrap();
    for f in &captures {
        assert!(
            f.new_allocation_request_bytes_upper_bound
                <= complete.new_allocation_request_bytes_upper_bound
        );
        assert!(
            f.allocation_request_count_upper_bound <= complete.allocation_request_count_upper_bound
        );
        assert!(
            f.cumulative_library_work_upper_bound <= complete.cumulative_library_work_upper_bound
        );
        assert_eq!(f.source_retained_bytes, source);
    }
    prefixes(&control.trace(), |c| {
        recursive_run(&input, &field, &ty, source, c, &mut |_| Ok(()))
    });
}

#[test]
fn ordinary_original_type_refusal_observes_only_caller_footer_and_prefixes() {
    let (input, field, _, source) = scalar_fixture();
    let wrong = FunctionValueType::new(DataType::Int32, true);
    let control = Control::good();
    let result = flat_run(&input, &field, &wrong, source, &control, &mut |_| Ok(()));
    assert!(result.is_err());
    assert_eq!(cause(&result), None);
    assert!(control.trace().last().unwrap().1 > 0);
    prefixes(&control.trace(), |c| {
        flat_run(&input, &field, &wrong, source, c, &mut |_| Ok(()))
    });
}

#[test]
fn exact_complete_parent_invoice_replays_and_each_positive_axis_refuses_one_under() {
    let (input, field, ty, source) = scalar_fixture();
    let mut captures = Vec::new();
    let control = Control::good();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let mut capture = |f: &IpcReaderProgressFacts| {
        captures.push(*f);
        Ok(())
    };
    let stream = preflight_flat_constant_stream_in(
        &input,
        &field,
        stream_limits(),
        &verifier(),
        source,
        &mut capture,
        &mut work,
    )
    .unwrap();
    let prepared = stream
        .prepare_pool_borrowed_in(
            Arc::clone(&field),
            &ty,
            source,
            policy(),
            reader_limits(),
            &mut capture,
            &mut work,
        )
        .unwrap();
    let full = *prepared.facts();
    let complete = IpcReaderProgressFacts {
        source_retained_bytes: full.source_retained_bytes,
        geometry_scratch_request_bytes: 0,
        geometry_scratch_request_count: 0,
        new_allocation_request_bytes_upper_bound: full.new_allocation_request_bytes_upper_bound,
        allocation_request_count_upper_bound: full.allocation_request_count_upper_bound,
        cumulative_library_work_upper_bound: full.cumulative_library_work_upper_bound,
    };
    prepared.materialize_in(&mut capture, &mut work).unwrap();
    work.finish().unwrap();
    assert!(complete.new_allocation_request_bytes_upper_bound > 0);
    assert!(complete.allocation_request_count_upper_bound > 0);
    assert!(complete.cumulative_library_work_upper_bound > 0);
    for f in &captures {
        assert!(
            f.new_allocation_request_bytes_upper_bound
                <= complete.new_allocation_request_bytes_upper_bound
        );
        assert!(
            f.allocation_request_count_upper_bound <= complete.allocation_request_count_upper_bound
        );
        assert!(
            f.cumulative_library_work_upper_bound <= complete.cumulative_library_work_upper_bound
        );
    }
    let select = |f: &IpcReaderProgressFacts, axis| match axis {
        0 => f.new_allocation_request_bytes_upper_bound,
        1 => f.allocation_request_count_upper_bound,
        2 => f.cumulative_library_work_upper_bound,
        _ => f
            .source_retained_bytes
            .checked_add(f.new_allocation_request_bytes_upper_bound)
            .unwrap(),
    };
    for axis in 0..4 {
        let bound = select(&complete, axis);
        let mut exact = |f: &IpcReaderProgressFacts| {
            if select(f, axis) > bound {
                Err(CompileControlError::ResourceExhausted)
            } else {
                Ok(())
            }
        };
        assert!(flat_run(&input, &field, &ty, source, &Control::good(), &mut exact).is_ok());
        let mut tight = |f: &IpcReaderProgressFacts| {
            if select(f, axis) >= bound {
                Err(CompileControlError::ResourceExhausted)
            } else {
                Ok(())
            }
        };
        assert_eq!(
            cause(&flat_run(
                &input,
                &field,
                &ty,
                source,
                &Control::good(),
                &mut tight
            )),
            Some(CompileControlError::ResourceExhausted)
        );
    }
}

#[test]
fn geometry_request_follows_observed_field_node_validation() {
    let (input, field, _, _) = nested_fixture();
    let good = Control::good();
    let mut parse = CompileCheckpoints::try_new(&good, CompilePhase::Decode).unwrap();
    let (raw, start) = metadata(&input, 0, 1 << 20, &mut parse).unwrap();
    let message =
        crate::ipc_flat_stream_v2::verified_record_message_observed(raw, &verifier(), &mut parse)
            .unwrap();
    let len = usize::try_from(message.bodyLength()).unwrap();
    let body = &input[start..start + len];
    let bytes = Layout::array::<crate::ipc_recursive_batch_v2::RecursiveNodeGeometry<'_>>(3)
        .unwrap()
        .size();
    for pending in [0, 254, 255] {
        for c in CAUSES {
            let cause = c.clone();
            let control = Control::refusing(1, c);
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
            for _ in 0..pending {
                work.step().unwrap();
            }
            let mut limits = recursive_limits().batch;
            limits.max_geometry_request_bytes = bytes - 1;
            let mut accept = |_: &IpcReaderProgressFacts| Ok(());
            let mut parent = progress::Admission::new(input.capacity() + (1 << 16), &mut accept);
            let result =
                crate::ipc_recursive_batch_v2::preflight_verified_recursive_record_batch_in(
                    message,
                    body,
                    &field,
                    limits,
                    &mut parent,
                    &mut work,
                );
            let trace = control.trace();
            if trace.len() == 1 {
                assert!(matches!(
                    result,
                    Err(TypeCodecError::Control(
                        CompileControlError::ResourceExhausted
                    ))
                ));
            } else {
                assert!(matches!(result, Err(TypeCodecError::Control(actual)) if actual == cause));
                assert_eq!(trace.len(), 2);
            }
        }
    }
}

#[test]
fn source_invoice_and_foreign_consume_fail_ordinary_without_parent_fallback() {
    let (input, field, ty, source) = scalar_fixture();
    let control = Control::good();
    let result = flat_run(&input, &field, &ty, input.len() - 1, &control, &mut |_| {
        panic!("source floor refusal must precede parent callback")
    });
    assert!(result.is_err());
    assert_eq!(cause(&result), None);
    let control = Control::good();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let stream = preflight_flat_constant_stream_in(
        &input,
        &field,
        stream_limits(),
        &verifier(),
        source,
        &mut |_| Ok(()),
        &mut work,
    )
    .unwrap();
    let prepared = stream
        .prepare_pool_borrowed_in(
            Arc::clone(&field),
            &ty,
            source,
            policy(),
            reader_limits(),
            &mut |_| Ok(()),
            &mut work,
        )
        .unwrap();
    let foreign = Control::good();
    let mut other = CompileCheckpoints::try_new(&foreign, CompilePhase::Decode).unwrap();
    let result = prepared.materialize_in(
        &mut |_| panic!("foreign consume must precede admission"),
        &mut other,
    );
    assert!(result.is_err());
    assert_eq!(cause(&result), None);
    assert_eq!(foreign.trace(), vec![(CompilePhase::Decode, 0)]);
}

#[test]
fn actual_metadata_verifier_sum_overflow_precedes_pending_controller_refusal() {
    let (input, field, _, source) = scalar_fixture();
    for pending in [0, 254, 255] {
        for c in CAUSES {
            let control = Control::refusing(1, c);
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
            for _ in 0..pending {
                work.step().unwrap();
            }
            let mut verify = verifier();
            verify.max_apparent_size = usize::MAX;
            let result = preflight_flat_constant_stream_in(
                &input,
                &field,
                stream_limits(),
                &verify,
                source,
                &mut |_| Ok(()),
                &mut work,
            );
            assert!(matches!(
                result,
                Err(TypeCodecError::Control(
                    CompileControlError::ResourceExhausted
                ))
            ));
            assert_eq!(control.trace(), vec![(CompilePhase::Decode, 0)]);
        }
    }
}
