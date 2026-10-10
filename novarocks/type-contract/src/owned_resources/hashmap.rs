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

//! Locked std HashMap request geometry and closed opaque key-operation work.
//! This is a numerical source projection, never an allocator or CPU grant.

use super::profile::LOCKED_TOOLCHAIN;
use std::alloc::Layout;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HashMapResourceError {
    SourceModel(&'static str),
    Arithmetic(&'static str),
}
use HashMapResourceError::{Arithmetic, SourceModel};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FreshTableFacts {
    pub layout: Option<Layout>,
    pub buckets: usize,
    pub allocation_requests_upper_bound: usize,
    pub request_bytes_upper_bound: usize,
}

/// Cumulative requests of an originally empty insertion-only table. Calls,
/// rather than distinct keys, bound growth even when a duplicate is searched
/// after a reserve. This describes new library requests, never a retained map.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FreshInsertionFacts {
    pub insertion_calls: usize,
    pub final_bucket_upper_bound: usize,
    pub allocation_requests_upper_bound: usize,
    pub request_bytes_upper_bound: usize,
}
fn add(a: usize, b: usize) -> Result<usize, HashMapResourceError> {
    a.checked_add(b)
        .ok_or(Arithmetic("HashMap resource sum overflow"))
}
fn mul(a: usize, b: usize) -> Result<usize, HashMapResourceError> {
    a.checked_mul(b)
        .ok_or(Arithmetic("HashMap resource product overflow"))
}

/// Rust 1.98.1 std Cargo.lock selects hashbrown 0.17.1 with
/// rustc-dep-of-std/nightly. control/group/mod.rs selects these public
/// intrinsic carriers, or the actual generic u64 word on the proven targets.
/// No private Group repr is recreated. Other targets need a source review.
fn group_layout() -> Result<Layout, HashMapResourceError> {
    if !LOCKED_TOOLCHAIN {
        return Err(SourceModel(
            "HashMap allocation toolchain source model drift",
        ));
    }
    if std::mem::size_of::<usize>() != 8
        || Layout::new::<usize>() != Layout::new::<Option<std::ptr::NonNull<()>>>()
    {
        return Err(SourceModel("HashMap pointer target source model drift"));
    }
    #[cfg(all(not(miri), target_arch = "x86_64", target_feature = "sse2"))]
    {
        Ok(Layout::new::<core::arch::x86_64::__m128i>())
    }
    #[cfg(all(
        not(miri),
        target_arch = "aarch64",
        target_feature = "neon",
        target_endian = "little"
    ))]
    {
        Ok(Layout::new::<core::arch::aarch64::uint8x8_t>())
    }
    #[cfg(all(
        any(target_arch = "aarch64", all(miri, target_arch = "x86_64")),
        not(any(
            all(not(miri), target_arch = "x86_64", target_feature = "sse2"),
            all(
                not(miri),
                target_arch = "aarch64",
                target_feature = "neon",
                target_endian = "little"
            )
        ))
    ))]
    {
        Ok(Layout::new::<u64>())
    }
    // A consumer disabling SSE2 does not rebuild its precompiled sysroot.
    // Without a build-std source loan its std table can still use Group16.
    #[cfg(all(not(miri), target_arch = "x86_64", not(target_feature = "sse2")))]
    {
        Err(SourceModel(
            "HashMap precompiled x86_64 Group source model is unsupported",
        ))
    }
    #[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
    {
        Err(SourceModel(
            "HashMap Group target source model is unsupported",
        ))
    }
}

/// Only an empty HashMap followed by one reserve/try_reserve(unique_count),
/// or with_capacity(unique_count), and at most that many insertion occurrences.
/// Rust 1.98.1 std's ONE-entry array From route uses empty + extend + reserve(1).
/// These constructors share RawTable capacity_to_buckets/TableLayout below.
/// No removals, prior allocation, mutable escape, or second growth are covered.
/// raw/mod.rs capacity_to_buckets/TableLayout are the sole source formula.
/// RandomState construction/OS seed acquisition is separately caller-owned.
pub fn fresh_table_layout<K, V>(
    unique_count: usize,
) -> Result<FreshTableFacts, HashMapResourceError> {
    let group = group_layout()?;
    if unique_count == 0 {
        return Ok(FreshTableFacts {
            layout: None,
            buckets: 0,
            allocation_requests_upper_bound: 0,
            request_bytes_upper_bound: 0,
        });
    }
    let pair = Layout::new::<(K, V)>();
    let width = group.size();
    if !matches!(width, 8 | 16) || group.align() > width {
        return Err(SourceModel(
            "HashMap Group intrinsic layout source model drift",
        ));
    }
    let buckets = if unique_count < 15 {
        let min_cap = match (width, pair.size()) {
            (16, 0..=1) => 14,
            (16, 2..=3) | (8, 0..=1) => 7,
            _ => 3,
        };
        let capacity = unique_count.max(min_cap);
        if capacity < 4 {
            4
        } else if capacity < 8 {
            8
        } else {
            16
        }
    } else {
        (mul(unique_count, 8)? / 7)
            .checked_next_power_of_two()
            .ok_or(Arithmetic("HashMap bucket count overflow"))?
    };
    let align = pair.align().max(width);
    let ctrl_offset = add(mul(pair.size(), buckets)?, align - 1)? & !(align - 1);
    let size = add(ctrl_offset, add(buckets, width)?)?;
    // hashbrown requires this strict margin before its unchecked Layout.
    if size
        > (isize::MAX as usize)
            .checked_sub(align - 1)
            .ok_or(Arithmetic("HashMap alignment extent overflow"))?
    {
        return Err(Arithmetic("HashMap allocation layout is unrepresentable"));
    }
    let layout = Layout::from_size_align(size, align)
        .map_err(|_| Arithmetic("HashMap allocation layout is unrepresentable"))?;
    // The real raw table request is not padded to Layout alignment.
    Ok(FreshTableFacts {
        layout: Some(layout),
        buckets,
        allocation_requests_upper_bound: 1,
        request_bytes_upper_bound: size,
    })
}

/// The original `HashMap::new` followed by at most `insertion_calls` insert or
/// entry operations, without removals, reserve, mutable escape or rebuilding.
/// Values and keys remain separately caller-authored; RandomState/OS seeding
/// is not an allocator/CPU promise from this numerical table projection.
///
/// Rust 1.98.1 hashbrown 0.17.1 reserve_rehash_inner grows to
/// capacity_to_buckets(max(items + 1, full_capacity + 1)). With no deletions,
/// allocated bucket counts increase through a power-of-two subsequence from
/// the first table through capacity_to_buckets(insertion_calls). Enumerating
/// every such extent dominates duplicates and early exits, including old/new
/// coexistence during resize. Each extent uses the SAME table-layout author
/// as the one-reserve port above; that port's single-reserve contract is not
/// reinterpreted as a cumulative request bound.
pub fn original_fresh_insertion_allocation_requests_observed<
    K,
    V,
    E: From<HashMapResourceError>,
>(
    insertion_calls: usize,
    allocation: &mut impl FnMut(Layout) -> Result<(), E>,
) -> Result<FreshInsertionFacts, E> {
    let last = fresh_table_layout::<K, V>(insertion_calls).map_err(E::from)?;
    let mut result = FreshInsertionFacts {
        insertion_calls,
        final_bucket_upper_bound: last.buckets,
        allocation_requests_upper_bound: 0,
        request_bytes_upper_bound: 0,
    };
    if insertion_calls == 0 {
        return Ok(result);
    }
    let mut table = fresh_table_layout::<K, V>(1).map_err(E::from)?;
    loop {
        let layout = table
            .layout
            .ok_or_else(|| E::from(SourceModel("fresh HashMap insertion table has no layout")))?;
        result.allocation_requests_upper_bound =
            add(result.allocation_requests_upper_bound, 1).map_err(E::from)?;
        result.request_bytes_upper_bound =
            add(result.request_bytes_upper_bound, layout.size()).map_err(E::from)?;
        allocation(layout)?;
        if table.buckets == last.buckets {
            return Ok(result);
        }
        // The original bucket_mask_to_capacity is b-1 below eight buckets,
        // then 7/8 of b. Requesting one beyond it selects the next table.
        let full_capacity = if table.buckets < 8 {
            table.buckets - 1
        } else {
            mul(table.buckets / 8, 7).map_err(E::from)?
        };
        let next =
            fresh_table_layout::<K, V>(add(full_capacity, 1).map_err(E::from)?).map_err(E::from)?;
        if next.buckets <= table.buckets || next.buckets > last.buckets {
            return Err(E::from(SourceModel(
                "fresh HashMap insertion growth source model drift",
            )));
        }
        table = next;
    }
}

/// Only std String/&str keys and std RandomState/SipHasher13, not arbitrary
/// user Hash/Eq/BuildHasher code. bucket_upper_bound may be a truthful whole
/// retained source invoice for an existing table, never public capacity().
///
/// Sip13 hashes bytes plus a prefix-free sentinel and four final rounds.
/// 64 units/byte and 256/operation conservatively cover its bounded ordinary
/// word/tail/round source. A triangular probe visits every group; allowing
/// every bucket plus a whole Group and a longest-key comparison per candidate
/// covers total collisions, including small-table replicated control tags.
/// These are source operation units, not cooperative callbacks or wall time.
pub fn string_operations_work_upper_bound(
    bucket_upper_bound: usize,
    operations: usize,
    total_key_bytes: usize,
    max_key_bytes: usize,
) -> Result<usize, HashMapResourceError> {
    let group = group_layout()?.size();
    let hash = add(
        mul(256, operations)?,
        mul(64, add(total_key_bytes, operations)?)?,
    )?;
    let candidates = mul(operations, add(bucket_upper_bound, group)?)?;
    let comparisons = mul(candidates, add(32, mul(2, max_key_bytes)?)?)?;
    add(hash, comparisons)
}

/// Fresh String/String tables initialize actual control bytes and move each
/// actual inline pair once. String payload copying has its own caller author.
pub fn fresh_string_table_work_upper_bound(
    unique_count: usize,
    total_key_bytes: usize,
    max_key_bytes: usize,
) -> Result<usize, HashMapResourceError> {
    let table = fresh_table_layout::<String, String>(unique_count)?;
    if unique_count == 0 {
        return Ok(0);
    }
    let initialization = add(table.buckets, group_layout()?.size())?;
    add(
        add(
            initialization,
            mul(unique_count, std::mem::size_of::<(String, String)>())?,
        )?,
        string_operations_work_upper_bound(
            table.buckets,
            unique_count,
            total_key_bytes,
            max_key_bytes,
        )?,
    )
}

/// Only the standard `[u8; 32]` Hash/Eq implementation and RandomState's
/// SipHasher13, including transparent carriers delegating exactly that body.
/// This excludes arbitrary user Hash/Eq/BuildHasher or value processing.
///
/// Rust 1.98.1 core array/mod.rs delegates Hash to its slice; hash/mod.rs writes
/// a length prefix with the VALUE 32 through write_usize, then u8::hash_slice
/// writes the actual 32 bytes once. DefaultHasher forwards to SipHasher13.
/// On the locked 64-bit target this admits 8 prefix bytes plus 32 data bytes,
/// not String's sentinel. The same generous 64 units/byte and 256 overhead
/// cover ordinary hashing/finalization. Array equality's BytewiseEq raw_eq
/// reads at most both actual 32-byte sources. Every bucket plus one whole
/// Group is admitted as a candidate, even with total collisions.
///
/// An existing table's bucket upper bound must come from an actual retained
/// source invoice or another admitted table author, never public capacity().
/// This is numerical opaque work, not cooperation, CPU/allocator admission,
/// RandomState OS seed acquisition, or a wall-time guarantee.
pub fn byte_array32_operations_work_upper_bound(
    bucket_upper_bound: usize,
    operations: usize,
) -> Result<usize, HashMapResourceError> {
    let group = group_layout()?.size();
    let hash_bytes = mul(operations, add(std::mem::size_of::<usize>(), 32)?)?;
    let hash = add(mul(256, operations)?, mul(64, hash_bytes)?)?;
    let candidates = mul(operations, add(bucket_upper_bound, group)?)?;
    let comparisons = mul(candidates, add(32, mul(2, 32)?)?)?;
    add(hash, comparisons)
}

/// Fresh fixed-key tables use the same one-reserve/no-second-growth geometry.
/// Initialization covers the real control bytes and one move of each actual
/// inline key/value pair. V's Hash/Clone/Drop/user callbacks are not covered;
/// the caller retains ownership of any such operation or payload copying.
pub fn fresh_byte_array32_table_work_upper_bound<V>(
    unique_count: usize,
) -> Result<usize, HashMapResourceError> {
    let table = fresh_table_layout::<[u8; 32], V>(unique_count)?;
    if unique_count == 0 {
        return Ok(0);
    }
    let initialization = add(table.buckets, group_layout()?.size())?;
    add(
        add(
            initialization,
            mul(unique_count, std::mem::size_of::<([u8; 32], V)>())?,
        )?,
        byte_array32_operations_work_upper_bound(table.buckets, unique_count)?,
    )
}

/// HashMap iteration scans control tags for empty and deleted buckets too.
/// The truthful whole retained invoice includes at least one byte/raw bucket;
/// visible len or public capacity() does not bound a retained deleted table.
/// This also admits the final None and visible-entry bookkeeping. Caller must
/// bracket the real iterator operations; there is no simulated byte loop here.
pub fn source_iterator_work_upper_bound(
    source_retained_bytes: usize,
    visible_entries: usize,
) -> Result<usize, HashMapResourceError> {
    let group = group_layout()?.size();
    add(
        mul(32, add(source_retained_bytes, group)?)?,
        mul(16, add(visible_entries, 1)?)?,
    )
}

#[cfg(test)]
#[path = "hashmap/tests.rs"]
mod tests;
