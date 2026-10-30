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

//! The private routing/table layout for the locked hashbrown adapter.
//! Mutable incremental indexes use identity (P=1). Bulk uses one closed input
//! bound and becomes read-only at Ready; this bound is not a user quota.

#[derive(Clone, Copy, Debug)]
pub(super) struct HashLayout {
    route_shift: u32,
    route_bits: u32,
    // Only a closed bulk build has this input fact. Incremental indexes have no
    // such bound and no route-bit exclusion from their table hash.
    bulk: Option<BulkGrowth>,
}
#[derive(Clone, Copy, Debug)]
struct BulkGrowth {
    eligible_rows: usize,
    bucket_bits_upper: Option<u32>,
}
impl HashLayout {
    pub(super) fn incremental() -> Self {
        Self {
            route_shift: 0,
            route_bits: 0,
            bulk: None,
        }
    }

    /// max_route_bits is a private optimization request, not worker count.
    /// If the library's actual growth envelope leaves fewer independent bits,
    /// choose fewer partitions. Never reject input on an entropy preference.
    pub(super) fn for_bulk(max_route_bits: u32, eligible_rows: usize) -> Self {
        Self::bulk_for_word(max_route_bits, eligible_rows, usize::BITS.min(64))
    }
    fn bulk_for_word(max_route_bits: u32, eligible_rows: usize, word_bits: u32) -> Self {
        debug_assert!(word_bits == 32 || word_bits == 64);
        let bucket_bits_upper = bucket_bits_for_items(eligible_rows);
        let available = bucket_bits_upper
            .and_then(|bucket| word_bits.checked_sub(7)?.checked_sub(bucket))
            .unwrap_or(0);
        let route_bits = if eligible_rows == 0 {
            0
        } else {
            max_route_bits.min(available).min(usize::BITS - 1)
        };
        // Place the window centrally in the gap, above every possible bucket
        // bit and below all seven original h2 tag bits.
        let route_shift = if route_bits == 0 {
            0
        } else {
            bucket_bits_upper.expect("available gap requires a bound")
                + (available - route_bits) / 2
        };
        Self {
            route_shift,
            route_bits,
            bulk: Some(BulkGrowth {
                eligible_rows,
                bucket_bits_upper,
            }),
        }
    }
    /// Actual private layout equality, including the original closed growth
    /// fact. Matching geometry grants no completion or resource authority.
    pub(super) fn same_layout(self, other: Self) -> bool {
        self.route_shift == other.route_shift
            && self.route_bits == other.route_bits
            && self.bulk.map(|b| (b.eligible_rows, b.bucket_bits_upper))
                == other.bulk.map(|b| (b.eligible_rows, b.bucket_bits_upper))
    }
    pub(super) fn partitions(self) -> usize {
        1usize << self.route_bits
    }
    pub(super) fn closed_input_bound(self) -> Option<usize> {
        self.bulk.map(|bound| bound.eligible_rows)
    }
    fn route_mask(self) -> u64 {
        if self.route_bits == 0 {
            0
        } else {
            ((1u64 << self.route_bits) - 1) << self.route_shift
        }
    }
    pub(super) fn route(self, full_hash: u64) -> usize {
        ((full_hash & self.route_mask()) >> self.route_shift) as usize
    }
    pub(super) fn table_hash(self, full_hash: u64) -> u64 {
        // No seed, new digest or key-byte read. For P=1 this is identity.
        full_hash & !self.route_mask()
    }

    /// Before an exact public HashTable::try_reserve(additional), check that
    /// this closed build does not ask to reserve more classes than its input.
    /// A false result is an internal graph/accounting mismatch, not a product
    /// capacity refusal. The parent must not expose mutable intern after Ready.
    pub(super) fn admits_reservation(self, current_len: usize, additional: usize) -> bool {
        current_len.checked_add(additional).is_some_and(|needed| {
            self.bulk.is_none_or(|bound| {
                needed <= bound.eligible_rows
                    && (self.route_bits == 0
                        || bucket_bits_for_items(needed)
                            .zip(bound.bucket_bits_upper)
                            .is_some_and(|(actual, maximum)| actual <= maximum))
            })
        })
    }
}

/// Exact bucket rounding of locked hashbrown 0.14.5 raw::capacity_to_buckets.
/// This models only the monotone, insertion-only public HashTable sequence:
/// no erase/tombstones, no unbounded reserve hint, no with_capacity above the
/// closed input bound. The real allocator still decides every actual request.
fn bucket_bits_for_items(items: usize) -> Option<u32> {
    let buckets = if items == 0 {
        return Some(0);
    } else if items < 8 {
        if items < 4 { 4usize } else { 8usize }
    } else {
        (items.checked_mul(8)? / 7).checked_next_power_of_two()?
    };
    Some(buckets.ilog2())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn incremental_retains_full_hash_and_does_not_have_an_input_bound() {
        let layout = HashLayout::incremental();
        for bit in 0..64 {
            let value = 1u64 << bit;
            for hash in [0, value, !value, u64::MAX] {
                assert_eq!(layout.table_hash(hash), hash);
                assert_eq!(layout.route(hash), 0);
            }
        }
        assert_eq!(layout.partitions(), 1);
        assert!(layout.admits_reservation(0, usize::MAX));
        assert!(!layout.admits_reservation(usize::MAX, 1));
    }

    #[test]
    fn routing_and_table_bits_are_independent_and_preserve_bucket_and_tag_bits() {
        for word in [32u32, 64] {
            for upper in [1, 3, 4, 7, 8, 14, 15, 112, 113, 1025, 65_536] {
                for desired in [0, 1, 3, 7, 31, 63] {
                    let layout = HashLayout::bulk_for_word(desired, upper, word);
                    let mask = layout.route_mask();
                    let bucket = bucket_bits_for_items(upper).unwrap();
                    let low = (1u64 << bucket) - 1;
                    let tag = 0x7fu64 << (word - 7);
                    assert_eq!(mask & low, 0);
                    assert_eq!(mask & tag, 0);
                    for bit in 0..64 {
                        let basis = 1u64 << bit;
                        for hash in [0, !basis, u64::MAX] {
                            let changed = hash ^ basis;
                            assert_eq!(layout.table_hash(hash) & low, hash & low);
                            assert_eq!(layout.table_hash(hash) & tag, hash & tag);
                            if basis & mask == 0 {
                                assert_eq!(layout.route(hash), layout.route(changed));
                                assert_ne!(layout.table_hash(hash), layout.table_hash(changed));
                            } else {
                                assert_ne!(layout.route(hash), layout.route(changed));
                                assert_eq!(layout.table_hash(hash), layout.table_hash(changed));
                            }
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn every_route_can_have_every_small_bucket_and_tag_without_correlation() {
        let layout = HashLayout::for_bulk(3, 112);
        assert_eq!(layout.partitions(), 8);
        let tag_shift = usize::BITS.min(64) - 7;
        for route in 0..layout.partitions() {
            for bucket in 0..128u64 {
                for tag in 0..128u64 {
                    let full = ((route as u64) << layout.route_shift) | (tag << tag_shift) | bucket;
                    let table = layout.table_hash(full);
                    assert_eq!(layout.route(full), route);
                    assert_eq!(table & 127, bucket);
                    assert_eq!(table >> tag_shift, tag);
                }
            }
        }
    }

    #[test]
    fn growth_envelope_uses_exact_closed_input_not_the_ndv_hint_or_average_partition() {
        for eligible in [1, 7, 8, 17, 100, 1025, 65_536] {
            let layout = HashLayout::for_bulk(4, eligible);
            let max_bits = layout.bulk.unwrap().bucket_bits_upper.unwrap();
            for needed in 0..=eligible {
                assert!(layout.admits_reservation(needed, 0));
                assert!(bucket_bits_for_items(needed).unwrap() <= max_bits);
            }
            assert!(!layout.admits_reservation(eligible, 1));
        }
    }

    #[test]
    fn no_feasible_partition_gap_or_unrepresentable_upper_selects_identity() {
        let enormous = HashLayout::for_bulk(8, usize::MAX);
        assert_eq!(enormous.partitions(), 1);
        assert_eq!(enormous.table_hash(u64::MAX), u64::MAX);
        let empty = HashLayout::for_bulk(8, 0);
        assert_eq!(empty.partitions(), 1);
        let word32_no_gap = HashLayout::bulk_for_word(8, 1usize << 24, 32);
        assert_eq!(word32_no_gap.partitions(), 1);
        assert_eq!(word32_no_gap.table_hash(u64::MAX), u64::MAX);
    }
}

/// Known entry/control portion of the locked table's projected backing.
/// The private library's trailing group and alignment padding remain unknown;
/// this is deliberately not a complete Layout or allocation upper bound.
#[derive(Clone, Copy, Debug)]
pub(super) struct HashTableCost {
    #[expect(
        dead_code,
        reason = "Retains the original locked-table modeled bucket-count fact; it is not capacity authority."
    )]
    pub(super) buckets: usize,
    pub(super) entry_slot_bytes: u128,
    pub(super) control_byte_minimum: u128,
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Retains the explicit unknown hashbrown trailer/alignment caveat."
        )
    )]
    pub(super) unmodeled_trailer_and_alignment: bool,
}
impl HashLayout {
    pub(super) fn table_cost<T>(items: usize) -> Option<HashTableCost> {
        let buckets = if items == 0 {
            0
        } else {
            1usize.checked_shl(bucket_bits_for_items(items)?)?
        };
        // Reuse the original sole locked capacity-to-bucket model above.
        let entries = std::alloc::Layout::array::<T>(buckets).ok()?;
        Some(HashTableCost {
            buckets,
            entry_slot_bytes: entries.size() as u128,
            control_byte_minimum: buckets as u128,
            unmodeled_trailer_and_alignment: buckets != 0,
        })
    }
}
