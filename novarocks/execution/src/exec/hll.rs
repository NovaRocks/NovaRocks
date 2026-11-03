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

pub use novarocks_functions::datasketches_hll::*;

impl From<novarocks_functions::datasketches_hll_failure::ClassifiedHllFailure>
    for crate::runtime::task::ExecutionFailure
{
    fn from(source: novarocks_functions::datasketches_hll_failure::ClassifiedHllFailure) -> Self {
        Self::from_domain(source)
    }
}

#[cfg(test)]
mod tests {
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD;
    use datasketches::hll::{HllSketch, HllType};

    use super::{HllAccumulator, HllTargetType, hll_estimate};

    #[test]
    fn known_compact_payload_is_still_readable() {
        let payload = STANDARD
            .decode("AgEHEQMIAQQ9nPUc")
            .expect("decode base64 payload");

        assert_eq!(hll_estimate(&payload).expect("estimate"), 1);
        assert_eq!(
            HllAccumulator::from_payload_unreserved(&payload)
                .expect("accumulator from payload")
                .estimate()
                .expect("estimate from accumulator"),
            1
        );
    }

    #[test]
    fn legacy_coupon_union_with_typed_sql_string_is_two_and_replay_is_idempotent() {
        use crate::exec::sketch_hash::prehash_array_value;
        use arrow::array::{ArrayRef, StringArray};
        use std::sync::Arc;

        // Independent Java 6.2.0 reference: the old coupon is 0x1cf59c3d,
        // while SQL UTF-8 "1" prehashes to 0xd68cfa33ac865d67 and coupon 0x069586f8.
        let values: ArrayRef = Arc::new(StringArray::from(vec![Some("1"), None]));
        let hash = prehash_array_value(&values, 0, "test").unwrap().unwrap();
        assert_eq!(hash, 0xd68c_fa33_ac86_5d67);
        assert_eq!(prehash_array_value(&values, 1, "test").unwrap(), None);
        let legacy = STANDARD.decode("AgEHEQMIAQQ9nPUc").unwrap();
        let mut string = HllAccumulator::new_unreserved(10, HllTargetType::Hll6).unwrap();
        string.update_hash_unreserved(hash).unwrap();
        let string_payload = string.serialize().unwrap();
        assert_eq!(string_payload[6], 1);
        assert_eq!(&string_payload[8..12], &[0xf8, 0x86, 0x95, 0x06]);
        assert_eq!(string.estimate().unwrap(), 1);
        for reverse in [false, true] {
            let (first, second) = if reverse {
                (&string_payload, &legacy)
            } else {
                (&legacy, &string_payload)
            };
            let mut merged = HllAccumulator::from_payload_unreserved(first).unwrap();
            merged.merge_payload_unreserved(second).unwrap();
            merged.merge_payload_unreserved(first).unwrap();
            merged.merge_payload_unreserved(second).unwrap();
            assert_eq!(merged.estimate().unwrap(), 2);
            let payload = merged.serialize().unwrap();
            assert_eq!(payload[6], 2);
            let (coupon_bytes, remainder) = payload[8..16].as_chunks::<4>();
            assert!(remainder.is_empty());
            let mut coupons: Vec<_> = coupon_bytes
                .iter()
                .map(|bytes| u32::from_le_bytes(*bytes))
                .collect();
            coupons.sort_unstable();
            assert_eq!(coupons, vec![0x0695_86f8, 0x1cf5_9c3d]);
            assert_eq!(hll_estimate(&payload).unwrap(), 2);
        }
    }

    #[test]
    fn native_hll_roundtrip_merges_without_cpp() {
        let mut left =
            HllAccumulator::new_unreserved(10, HllTargetType::Hll6).expect("left accumulator");
        for value in 0_u64..64 {
            left.update_hash_unreserved(value).expect("update left");
        }
        let left_payload = left.serialize().expect("serialize left");

        let mut right =
            HllAccumulator::new_unreserved(10, HllTargetType::Hll6).expect("right accumulator");
        for value in 64_u64..128 {
            right.update_hash_unreserved(value).expect("update right");
        }
        let right_payload = right.serialize().expect("serialize right");

        let mut merged =
            HllAccumulator::from_payload_unreserved(&left_payload).expect("merged accumulator");
        merged
            .merge_payload_unreserved(&right_payload)
            .expect("merge right");

        let estimate = merged.estimate().expect("estimate merged");
        assert!(
            (110..=150).contains(&estimate),
            "merged estimate out of expected range: {estimate}"
        );
    }

    #[test]
    fn native_hll4_roundtrip_merges_without_cpp() {
        let mut sparse =
            HllAccumulator::new_unreserved(10, HllTargetType::Hll4).expect("sparse accumulator");
        for value in 0_u64..7 {
            sparse.update_hash_unreserved(value).expect("update sparse");
        }
        let sparse_sketch =
            HllSketch::deserialize(&sparse.serialize().expect("serialize sparse HLL4"))
                .expect("deserialize sparse HLL4");

        let mut left =
            HllAccumulator::new_unreserved(10, HllTargetType::Hll4).expect("left accumulator");
        for value in 0_u64..4_096 {
            left.update_hash_unreserved(value).expect("update left");
        }
        let left_payload = left.serialize().expect("serialize left");
        let left_sketch = HllSketch::deserialize(&left_payload).expect("deserialize left");
        assert_eq!(left_sketch.target_type(), HllType::Hll4);
        assert!(left_sketch.estimated_size() > sparse_sketch.estimated_size());

        let mut right =
            HllAccumulator::new_unreserved(10, HllTargetType::Hll4).expect("right accumulator");
        for value in 4_096_u64..8_192 {
            right.update_hash_unreserved(value).expect("update right");
        }
        let right_payload = right.serialize().expect("serialize right");
        let right_sketch = HllSketch::deserialize(&right_payload).expect("deserialize right");
        assert_eq!(right_sketch.target_type(), HllType::Hll4);

        let mut merged =
            HllAccumulator::from_payload_unreserved(&left_payload).expect("merged accumulator");
        merged
            .merge_payload_unreserved(&right_payload)
            .expect("merge right");
        let merged_payload = merged.serialize().expect("serialize merged");
        let merged_sketch = HllSketch::deserialize(&merged_payload).expect("deserialize merged");
        assert_eq!(merged_sketch.target_type(), HllType::Hll4);

        let roundtrip = HllAccumulator::from_payload_unreserved(&merged_payload)
            .expect("roundtrip accumulator");
        let roundtrip_payload = roundtrip.serialize().expect("serialize roundtrip");
        assert_eq!(
            HllSketch::deserialize(&roundtrip_payload)
                .expect("deserialize roundtrip")
                .target_type(),
            HllType::Hll4
        );

        let estimate = roundtrip.estimate().expect("estimate merged");
        assert!(
            (7_000..=9_500).contains(&estimate),
            "merged estimate out of expected range: {estimate}"
        );
    }
}

#[cfg(test)]
#[path = "legacy_ds_hll_decoder_error_baseline.rs"]
mod legacy_ds_hll_decoder_error_baseline;
