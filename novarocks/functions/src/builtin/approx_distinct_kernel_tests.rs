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
use arrow_array::Int64Array;
use std::{alloc::Layout, ptr::NonNull};

struct Host;
impl AggregateStateAllocator for Host {
    fn allocate(&self, layout: Layout) -> Result<NonNull<u8>, KernelFailure> {
        NonNull::new(unsafe { std::alloc::alloc(layout) }).ok_or(KernelFailure::ResourceExhausted)
    }

    unsafe fn release(&self, pointer: NonNull<u8>, layout: Layout) {
        unsafe { std::alloc::dealloc(pointer.as_ptr(), layout) }
    }
}

fn bigint_profile() -> HashProfile {
    HashProfile::prepare(&FunctionValueType::new(DataType::Int64, true)).unwrap()
}

fn date_profile() -> HashProfile {
    HashProfile::prepare(&FunctionValueType::new(DataType::Date32, true)).unwrap()
}

#[test]
fn approx_distinct_sparse_and_dense_merge_match_single_with_fixed_accuracy_sample() {
    let profile = bigint_profile();
    let allocator = HostAggregateAllocator::try_new(Arc::new(Host)).unwrap();
    let mut work = ScalarWork::new(None);
    let mut variance = 0.0;
    for group in 0..32 {
        let values = Int64Array::from_iter_values((1..=8192).map(|value| group * 8192 + value));
        let mut single = Sketch::new(DEFAULT_PRECISION, allocator.clone());
        let mut merged = Sketch::new(DEFAULT_PRECISION, allocator.clone());
        for partition in [0..64, 64..4096, 4096..8192] {
            let mut partial = Sketch::new(DEFAULT_PRECISION, allocator.clone());
            for row in partition {
                let hash = profile.hash(&values, row, &mut work).unwrap().unwrap();
                single.add_hash(hash, &mut work).unwrap();
                partial.add_hash(hash, &mut work).unwrap();
            }
            let bytes = partial
                .encode(1, profile.transport_profile(), &mut work)
                .unwrap();
            merged
                .merge(&bytes, 1, profile.transport_profile(), &mut work)
                .unwrap();
        }
        let estimate = single.estimate(&mut work).unwrap();
        assert_eq!(estimate, merged.estimate(&mut work).unwrap());
        assert_eq!(
            single
                .encode(1, profile.transport_profile(), &mut work)
                .unwrap()
                .as_slice(),
            merged
                .encode(1, profile.transport_profile(), &mut work)
                .unwrap()
                .as_slice(),
        );
        variance += (estimate as f64 / 8192.0 - 1.0).powi(2);
    }
    let rmse = (variance / 32.0).sqrt();
    assert!(rmse <= 0.046, "fixed-sample relative RMSE {rmse}");
}

#[test]
fn approx_distinct_uses_the_full_hash_and_rejects_foreign_state() {
    let profile = bigint_profile();
    let allocator = HostAggregateAllocator::try_new(Arc::new(Host)).unwrap();
    let mut sketch = Sketch::new(DEFAULT_PRECISION, allocator);
    let mut work = ScalarWork::new(None);
    for hash in [0, 0, u64::MAX, 1 << 63] {
        sketch.add_hash(hash, &mut work).unwrap();
    }
    assert_eq!(sketch.estimate(&mut work).unwrap(), 3);
    let bytes = sketch
        .encode(1, profile.transport_profile(), &mut work)
        .unwrap();
    assert!(
        sketch
            .merge(&bytes, 1, date_profile().transport_profile(), &mut work)
            .is_err()
    );
    assert!(
        sketch
            .merge(
                &[1, 2, 0, 0, 0, 0],
                1,
                profile.transport_profile(),
                &mut work
            )
            .is_err()
    );
}

#[test]
fn approx_distinct_kernel_freezes_only_valid_exact_accuracy() {
    assert_eq!(fixed_distinct_precision(1, None).unwrap(), Some(11));
    assert_eq!(fixed_distinct_precision(2, None).unwrap(), None);
    struct Compile;
    impl PureCompileControl for Compile {
        fn checkpoint(
            &self,
            _: CompilePhase,
            _: u32,
        ) -> Result<(), novarocks_type_contract::CompileControlError> {
            Ok(())
        }
    }

    let accuracy_type = FunctionValueType::new(DataType::Float64, true);
    let values = Float64Array::from(vec![
        Some(f64::NAN),
        Some(DEFAULT_ACCURACY),
        None,
        Some(MAX_ACCURACY.next_up()),
    ]);
    let pool = crate::ConstantPool::try_new(
        Arc::new(arrow_schema::Field::new("e", DataType::Float64, true)),
        accuracy_type.clone(),
        values.to_data(),
        crate::ConstantPolicy {
            max_rows: 4,
            max_array_nodes: 1,
            max_logical_elements: 4,
            max_retained_buffer_bytes: 1024,
            max_type_depth: 1,
            max_type_nodes: 1,
            max_dictionary_depth: 0,
            max_metadata_bytes: 1024,
            max_library_validation_work: 4 * 1024 * 1024,
            max_library_validation_bytes: 4 * 1024 * 1024,
        },
        CompilePhase::FunctionSpecialization,
        &Compile,
    )
    .unwrap();
    for (ordinal, expected) in [None, Some(11), None, None].into_iter().enumerate() {
        let argument = FunctionArgument::Value {
            value_type: accuracy_type.clone(),
            constant: Some(pool.value(ordinal as u32).unwrap()),
        };
        assert_eq!(
            fixed_distinct_precision(2, Some(&argument)).unwrap(),
            expected
        );
    }
    for (accuracy, precision) in [(0.26, 4), (0.023, 11), (0.004_062_5, 16)] {
        assert_eq!(precision_for_accuracy(accuracy), Some(precision));
        assert_eq!(
            first_precision(2, Some(precision), accuracy).unwrap(),
            precision
        );
    }
    assert_eq!(first_precision(1, Some(11), f64::NAN).unwrap(), 11);
    for invalid in [
        MIN_ACCURACY.next_down(),
        MAX_ACCURACY.next_up(),
        f64::NAN,
        f64::NEG_INFINITY,
    ] {
        assert_eq!(precision_for_accuracy(invalid), None);
        assert!(first_precision(2, None, invalid).is_err());
    }
    assert!(first_precision(2, Some(16), 0.023).is_err());
}

#[test]
fn approx_distinct_sketch_matches_airlift_451_default_sparse_and_dense_vectors() {
    let profile = bigint_profile();
    let allocator = HostAggregateAllocator::try_new(Arc::new(Host)).unwrap();
    let values = Int64Array::from_iter_values(1..=131_072);
    let mut work = ScalarWork::new(None);
    // Single Airlift 451 sketches receive Trino XX_HASH_64(BIGINT) for
    // sequential integers starting at 1. The representation tags below
    // use NovaRocks' v2 encoding (0 sparse, 1 dense).
    for (precision, expected) in [
        (
            4,
            &[
                (0, 0, 0),
                (1, 1, 0),
                (2, 2, 1),
                (4, 5, 1),
                (24, 25, 1),
                (8192, 6342, 1),
            ][..],
        ),
        (
            11,
            &[
                (0, 0, 0),
                (251, 251, 0),
                (252, 247, 1),
                (4096, 4018, 1),
                (8192, 8237, 1),
            ][..],
        ),
        (
            16,
            &[
                (0, 0, 0),
                (8191, 8191, 0),
                (8192, 8238, 1),
                (16384, 16421, 1),
                (131072, 131450, 1),
            ][..],
        ),
    ] {
        let mut next = 0;
        let mut sketch = Sketch::new(precision, allocator.clone());
        for &(count, estimate, representation) in expected {
            while next < count {
                let hash = profile.hash(&values, next, &mut work).unwrap().unwrap();
                sketch.add_hash(hash, &mut work).unwrap();
                next += 1;
            }
            assert_eq!(
                sketch.estimate(&mut work).unwrap(),
                estimate,
                "p{precision}, {count} inputs"
            );
            let state = sketch
                .encode(2, profile.transport_profile(), &mut work)
                .unwrap();
            assert_eq!(state[5], representation, "p{precision}, {count} inputs");
        }
    }
}

#[test]
fn approx_distinct_sketch_has_fixed_precision_neutral_empty_and_exact_origin() {
    let profile = bigint_profile();
    let recipe = profile.transport_profile();
    let allocator = HostAggregateAllocator::try_new(Arc::new(Host)).unwrap();
    let mut work = ScalarWork::new(None);
    let values = Int64Array::from_iter_values(1..=9000);
    for precision in [4, 11, 16] {
        let mut source = Sketch::new(precision, allocator.clone());
        let empty = source.encode(2, recipe, &mut work).unwrap();
        let mut single = Sketch::new(precision, allocator.clone());
        let mut partials = Vec::new();
        for rows in [0..50, 50..250, 250..3000, 3000..9000] {
            let mut partial = Sketch::new(precision, allocator.clone());
            for row in rows {
                let hash = profile.hash(&values, row, &mut work).unwrap().unwrap();
                partial.add_hash(hash, &mut work).unwrap();
                single.add_hash(hash, &mut work).unwrap();
            }
            partials.push(partial.encode(2, recipe, &mut work).unwrap());
        }
        source.merge(&empty, 2, recipe, &mut work).unwrap();
        for state in partials.iter().rev() {
            source.merge(state, 2, recipe, &mut work).unwrap();
        }
        source.merge(&empty, 2, recipe, &mut work).unwrap();
        let result = source.encode(2, recipe, &mut work).unwrap();
        assert_eq!(result, single.encode(2, recipe, &mut work).unwrap());
        assert_eq!(
            source.estimate(&mut work).unwrap(),
            single.estimate(&mut work).unwrap()
        );

        assert!(source.merge(&result, 1, recipe, &mut work).is_err());
        assert!(
            source
                .merge(&result, 2, date_profile().transport_profile(), &mut work)
                .is_err()
        );
        let wrong_precision = if precision == 16 { 4 } else { 16 };
        let mut wrong_precision = Sketch::new(wrong_precision, allocator.clone());
        assert!(
            wrong_precision
                .merge(&result, 2, recipe, &mut work)
                .is_err()
        );
        assert!(
            source
                .merge(b"NH\x01\x0b\x04\x00", 2, recipe, &mut work)
                .is_err()
        );
    }
}
