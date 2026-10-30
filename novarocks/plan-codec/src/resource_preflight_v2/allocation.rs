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

//! Requested-size bounds for the audited Rust 1.98.1 RawVec and fresh Prost 0.13.5
//! decoding from &[u8], with bytes 1.11.1. These are not allocator RSS or a
//! grant. Each contribution counts all occurrences, including overwritten
//! objects. Old blocks already occur in the cumulative allocation sum.

use super::ResourceModelError as E;

pub(super) fn checked_add(left: usize, right: usize) -> Result<usize, E> {
    left.checked_add(right)
        .ok_or(E::Limit("resource size addition overflow"))
}
pub(super) fn checked_mul(left: usize, right: usize) -> Result<usize, E> {
    left.checked_mul(right)
        .ok_or(E::Limit("resource size multiplication overflow"))
}

/// RawVec::grow_amortized doubles capacity. For N pushes of a non-ZST,
/// requested capacities sum to (2*C - M), where C is final capacity and M
/// is its first capacity. Above M, C < 2*N; at/below M, only M is requested.
/// Hence max(4, M)*N*S bounds every new request, including old/new overlap.
/// The input may conservatively count pushes to different/replaced vectors:
/// applying the same linear contribution to each occurrence remains safe.
pub(super) fn repeated_slots(element_size: usize, occurrences: usize) -> Result<usize, E> {
    let min_capacity = match element_size {
        0 => return Ok(0),
        1 => 8,
        2..=1024 => 4,
        _ => 1,
    };
    checked_mul(checked_mul(element_size, min_capacity.max(4))?, occurrences)
}

/// String and Vec-byte replacement clear the previous logical length, then
/// reserve the new length. A growth request is at most max(8, 2*new_len):
/// growth implies new_len > old_capacity. This wider per-occurrence bound
/// also covers String formatting/copy growth when used with a proven length.
pub(super) fn replacement_bytes(length: usize) -> Result<usize, E> {
    if length == 0 {
        return Ok(0);
    }
    checked_add(8, checked_mul(4, length)?)
}

/// Prost bytes::merge first makes a Bytes temporary. For a concrete &[u8]
/// source, bytes 1.11.1 uses BytesMut::with_capacity(len), whose exact-length
/// Vec freezes into a boxed slice without a Shared header. The temporary
/// backing coexists with destination growth; String merge_one_copy has none.
pub(super) fn slice_bytes_payload(length: usize) -> Result<usize, E> {
    checked_add(replacement_bytes(length)?, length)
}

/// The pinned DecodeError Inner has a Cow description and a Vec of static
/// context pairs. Bound field alignment without mirroring the private type.
/// Its three dynamic formatting paths request at most 114 bytes cumulatively:
/// invalid key (38 then 76), invalid wire value (50), wrong wire type (62).
/// Oneof propagation does not add context; message propagation adds at most
/// one pair per message level. Display/clone/io::Error conversion are excluded.
pub(super) fn error_heap(max_message_depth: usize) -> Result<usize, E> {
    use std::{
        borrow::Cow,
        mem::{align_of, size_of},
    };
    type Pair = (&'static str, &'static str);
    let alignment = align_of::<Cow<'static, str>>().max(align_of::<Vec<Pair>>());
    let fields = checked_add(size_of::<Cow<'static, str>>(), size_of::<Vec<Pair>>())?;
    let padded = checked_add(fields, checked_mul(2, alignment - 1)?)?;
    let inner = checked_add(padded, alignment - 1)? / alignment * alignment;
    checked_add(
        checked_add(inner, 114)?,
        repeated_slots(size_of::<Pair>(), max_message_depth)?,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn requests<T: Clone>(value: T, count: usize) -> usize {
        let mut values = Vec::new();
        let mut total = 0;
        for _ in 0..count {
            let before = values.capacity();
            values.push(value.clone());
            if values.capacity() != before {
                total += values.capacity() * std::mem::size_of::<T>();
            }
        }
        total
    }

    #[test]
    fn actual_rawvec_requests_fit_all_three_initial_capacity_branches() {
        for count in [0, 1, 2, 3, 4, 5, 8, 9, 16, 17, 31, 32, 33, 65, 129] {
            assert!(requests(0u8, count) <= repeated_slots(1, count).unwrap());
            assert!(requests(0u64, count) <= repeated_slots(8, count).unwrap());
            assert!(requests([0u8; 1024], count) <= repeated_slots(1024, count).unwrap());
            assert!(requests([0u8; 1025], count) <= repeated_slots(1025, count).unwrap());
            assert!(requests((), count) <= repeated_slots(0, count).unwrap());
        }
    }

    #[test]
    fn actual_string_replacements_keep_old_capacity_within_occurrence_sum() {
        let mut value = String::new();
        let mut requests = 0usize;
        let mut bound = 0usize;
        for len in [0, 1, 2, 9, 3, 17, 0, 33, 1, 257, 65, 513, 0] {
            bound += replacement_bytes(len).unwrap();
            let previous = value.capacity();
            value.clear();
            value.reserve(len);
            value.extend(std::iter::repeat_n('x', len));
            if value.capacity() != previous {
                requests += value.capacity();
            }
            assert!(requests <= bound);
        }
    }

    #[test]
    fn bytes_slice_temporary_is_counted_independently_of_destination() {
        assert_eq!(replacement_bytes(0).unwrap(), 0);
        assert_eq!(slice_bytes_payload(0).unwrap(), 0);
        assert_eq!(replacement_bytes(7).unwrap(), 36);
        assert_eq!(slice_bytes_payload(7).unwrap(), 43);
        assert_eq!(repeated_slots(4096, 2).unwrap(), 32_768);
    }

    #[test]
    fn all_size_operations_refuse_overflow_without_saturating_to_a_budget() {
        assert!(checked_add(usize::MAX, 1).is_err());
        assert!(checked_mul(usize::MAX, 2).is_err());
        assert!(repeated_slots(usize::MAX, 1).is_err());
        assert!(repeated_slots(1, usize::MAX).is_err());
        assert!(replacement_bytes(usize::MAX).is_err());
        assert!(slice_bytes_payload(usize::MAX).is_err());
        assert_eq!(repeated_slots(0, usize::MAX).unwrap(), 0);
        assert!(error_heap(usize::MAX).is_err());
    }

    #[test]
    fn error_context_budget_counts_static_pairs_and_owned_description_separately() {
        let base = error_heap(0).unwrap();
        let twelve = error_heap(12).unwrap();
        assert_eq!(twelve - base, 48 * std::mem::size_of::<(&str, &str)>());
        assert!(base >= 114 + std::mem::size_of::<std::borrow::Cow<'static, str>>());
    }
}
