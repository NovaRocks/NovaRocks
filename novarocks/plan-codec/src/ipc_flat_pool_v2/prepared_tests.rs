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

fn positive(control: &Control) -> Vec<u32> {
    control
        .trace()
        .into_iter()
        .filter_map(|(_, units)| (units > 0).then_some(units))
        .collect()
}
fn every_boundary<T>(call: impl Fn(&Control) -> Result<T, TypeCodecError>, successful: bool) {
    let good = Control::good(CompilePhase::Encode);
    assert_eq!(call(&good).is_ok(), successful);
    let trace = good.trace();
    assert_eq!(trace.first(), Some(&(CompilePhase::Encode, 0)));
    for at in 0..trace.len() {
        for cause in CAUSES {
            let control = Control::refusing(at, cause.clone());
            assert!(
                matches!(call(&control), Err(TypeCodecError::Control(actual)) if actual == cause)
            );
            assert_eq!(control.trace(), trace[..=at]);
        }
    }
}
#[test]
fn prepared_flat_writer_preserves_original_pool_slice_and_single_numerical_pass() {
    let numeric: ArrayRef = Arc::new(Float32Array::from(vec![
        Some(1.0),
        Some(f32::from_bits(0x7fc1_2345)),
        None,
        Some(-0.0),
    ]));
    let bytes: ArrayRef = Arc::new(StringArray::from(vec![
        Some("unused"),
        Some("selected"),
        None,
        Some("last"),
    ]));
    for array in [numeric.slice(1, 3), bytes.slice(1, 3)] {
        let original = pool(array);
        let identity = original.backing_identity();
        let baseline = Control::good(CompilePhase::Encode);
        let expected =
            encode_flat_pool(&original, invoice(&original), limits(), &baseline).unwrap();
        let preparation = Control::good(CompilePhase::Encode);
        let prepared =
            prepare_flat_pool_write(&original, invoice(&original), limits(), &preparation).unwrap();
        assert!(std::ptr::eq(prepared.pool, &original));
        assert_eq!(prepared.facts().rows, 3);
        assert!(prepared.facts().encoded_stream_bytes_upper_bound >= expected.len());
        let emission = Control::good(CompilePhase::Encode);
        let output = prepared.emit(&emission).unwrap();
        assert_eq!(output, expected);
        assert_eq!(original.backing_identity(), identity);
        compare_standard_batch(&output, &standard(&original), original.field().data_type());
        let actual = materialize(&output, &original);
        assert!(
            original
                .value(2)
                .unwrap()
                .equals_observed(
                    &actual.value(2).unwrap(),
                    CompilePhase::Decode,
                    &Control::good(CompilePhase::Decode)
                )
                .unwrap()
        );
        let mut split = positive(&preparation);
        split.extend(positive(&emission));
        assert_eq!(split, positive(&baseline));
    }
}
#[test]
fn prepared_flat_writer_preparation_success_and_resource_tail_keep_all_original_causes() {
    let original = pool(Arc::new(Int64Array::from(vec![Some(7), None, Some(-9)])));
    for successful in [true, false] {
        let admitted = if successful {
            limits()
        } else {
            FlatPoolWriteLimits {
                max_new_allocation_request_bytes: 0,
                ..limits()
            }
        };
        every_boundary(
            |control| prepare_flat_pool_write(&original, invoice(&original), admitted, control),
            successful,
        );
    }
}
#[test]
fn prepared_flat_writer_emission_success_and_internal_shape_tail_keep_all_original_causes() {
    let original = pool(Arc::new(Int64Array::from(vec![Some(7), None, Some(-9)])));
    for successful in [true, false] {
        every_boundary(
            |control| {
                let mut prepared = prepare_flat_pool_write(
                    &original,
                    invoice(&original),
                    limits(),
                    &Control::good(CompilePhase::Encode),
                )
                .unwrap();
                if !successful {
                    // Exercise the ordinary tail of a violated prepared
                    // extent without authorizing further allocation.
                    prepared.prepared.geometry.body_bytes = usize::MAX;
                }
                prepared.emit(control)
            },
            successful,
        );
    }
}
