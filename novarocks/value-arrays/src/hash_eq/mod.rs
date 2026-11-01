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

//! Logical-byte algorithms, independent of carrier and dictionary encoding.
//! Callers choose algorithm, seed and NULL semantics explicitly; collation and
//! multi-column combination remain the consumer's responsibility.
use crate::{
    ValueError, ValueFailure, ValueViolation,
    control::{Growth, Work},
    decode::DecodedBytes,
    reachability::SelectionSet,
};
use std::{alloc::Layout, convert::Infallible};
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ByteHashAlgorithm {
    /// The seed is the initial state (the conventional offset basis is
    /// 14695981039346656037). Each byte XOR precedes multiplication.
    Fnv1a64 { seed: u64 },
    /// IEEE reflected CRC32, with an already finalized previous CRC as seed.
    Crc32 { seed: u32 },
}
impl ByteHashAlgorithm {
    pub fn seed(self) -> u64 {
        match self {
            Self::Fnv1a64 { seed } => seed,
            Self::Crc32 { seed } => u64::from(seed),
        }
    }
    pub fn hash_bytes(self, bytes: &[u8]) -> u64 {
        use crate::batch::hash::{ByteHashKernel, Crc32, Fnv1a64};
        match self {
            Self::Fnv1a64 { seed } => {
                let mut state = Fnv1a64.init(seed);
                Fnv1a64.update(&mut state, bytes);
                Fnv1a64.finish(state)
            }
            Self::Crc32 { seed } => {
                let mut state = Crc32.init(seed);
                Crc32.update(&mut state, bytes);
                Crc32.finish(state)
            }
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NullHash {
    PreserveSeed,
    Value(u64),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NullEquality {
    Equal,
    Unequal,
    Unknown,
}
/// NULL-vs-non-NULL follows the same explicit policy as NULL-vs-NULL for SQL
/// UNKNOWN, while grouping equality distinguishes those cases.
pub fn equal_row(
    left: &DecodedBytes,
    left_row: usize,
    right: &DecodedBytes,
    right_row: usize,
    nulls: NullEquality,
) -> Result<Option<bool>, ValueError> {
    equal_row_observed(left, left_row, right, right_row, nulls, &mut |_| {
        Ok::<_, Infallible>(())
    })
    .map_err(ValueFailure::into_error)
}
/// Cancellation/control work is bounded independently of one value's length.
/// This never changes the consumer's NULL or collation policy.
pub fn equal_row_observed<E>(
    left: &DecodedBytes,
    left_row: usize,
    right: &DecodedBytes,
    right_row: usize,
    nulls: NullEquality,
    work: &mut Work<'_, E>,
) -> Result<Option<bool>, ValueFailure<E>> {
    if left.is_utf8() != right.is_utf8() {
        return Err(ValueError::Invariant(ValueViolation::Layout).into());
    }
    Ok(match (left.value(left_row)?, right.value(right_row)?) {
        (Some(a), Some(b)) => {
            if a.len() != b.len() {
                Some(false)
            } else {
                let mut equal = true;
                for (a, b) in a.chunks(64 * 1024).zip(b.chunks(64 * 1024)) {
                    work(a.len()).map_err(ValueFailure::Caller)?;
                    if a != b {
                        equal = false;
                        break;
                    }
                }
                Some(equal)
            }
        }
        (None, None) => match nulls {
            NullEquality::Equal => Some(true),
            NullEquality::Unequal => Some(false),
            NullEquality::Unknown => None,
        },
        _ => {
            if nulls == NullEquality::Unknown {
                None
            } else {
                Some(false)
            }
        }
    })
}
pub fn hash_row(
    input: &DecodedBytes,
    row: usize,
    algorithm: ByteHashAlgorithm,
    nulls: NullHash,
) -> Result<u64, ValueError> {
    hash_row_observed(input, row, algorithm, nulls, &mut |_| {
        Ok::<_, Infallible>(())
    })
    .map_err(ValueFailure::into_error)
}
/// Algorithm states compose across bounded byte chunks. CRC seeds are finalized
/// prior CRCs; FNV seeds are the unmodified previous accumulator.
pub fn hash_row_observed<E>(
    input: &DecodedBytes,
    row: usize,
    mut algorithm: ByteHashAlgorithm,
    nulls: NullHash,
    work: &mut Work<'_, E>,
) -> Result<u64, ValueFailure<E>> {
    let Some(bytes) = input.value(row)? else {
        return Ok(match nulls {
            NullHash::PreserveSeed => algorithm.seed(),
            NullHash::Value(value) => value,
        });
    };
    for chunk in bytes.chunks(64 * 1024) {
        work(chunk.len()).map_err(ValueFailure::Caller)?;
        let hash = algorithm.hash_bytes(chunk);
        algorithm = match algorithm {
            ByteHashAlgorithm::Fnv1a64 { .. } => ByteHashAlgorithm::Fnv1a64 { seed: hash },
            ByteHashAlgorithm::Crc32 { .. } => ByteHashAlgorithm::Crc32 { seed: hash as u32 },
        };
    }
    Ok(algorithm.seed())
}
/// Entries outside selection remain None and must not masquerade as a hash of
/// NULL. Every selected row is checked against the decoded readable domain.
pub fn hash_rows(
    input: &DecodedBytes,
    selection: &SelectionSet,
    algorithm: ByteHashAlgorithm,
    nulls: NullHash,
) -> Result<Vec<Option<u64>>, ValueError> {
    hash_rows_observed(
        input,
        selection,
        algorithm,
        nulls,
        &mut |_| Ok::<_, Infallible>(()),
        &mut |_| Ok::<_, Infallible>(()),
    )
    .map_err(ValueFailure::into_error)
}
pub fn hash_rows_observed<E>(
    input: &DecodedBytes,
    selection: &SelectionSet,
    algorithm: ByteHashAlgorithm,
    nulls: NullHash,
    growth: &mut Growth<'_, E>,
    work: &mut Work<'_, E>,
) -> Result<Vec<Option<u64>>, ValueFailure<E>> {
    use crate::batch::{
        PreparedBytes, RowWork,
        hash::{Crc32, Fnv1a64, HashOptions, HashScratch, HashSeed, hash_into},
    };
    if selection.len() != input.len() {
        return Err(ValueError::Invariant(ValueViolation::UndefinedRow).into());
    }
    let prepared = PreparedBytes::new_observed(input, selection, &mut |n| work(n))?;
    let mut values = initialized_result(prepared.len(), 0u64, growth, work)?;
    let mut scratch = HashScratch::new();
    match algorithm {
        ByteHashAlgorithm::Fnv1a64 { seed } => hash_into(
            &prepared,
            &Fnv1a64,
            HashOptions {
                seeds: HashSeed::Uniform(seed),
                nulls,
            },
            &mut values,
            &mut scratch,
            &mut |layout| growth(layout),
            &mut |n| work(n),
        )?,
        ByteHashAlgorithm::Crc32 { seed } => hash_into(
            &prepared,
            &Crc32,
            HashOptions {
                seeds: HashSeed::Uniform(seed),
                nulls,
            },
            &mut values,
            &mut scratch,
            &mut |layout| growth(layout),
            &mut |n| work(n),
        )?,
    }
    let mut out = initialized_result(input.len(), None, growth, work)?;
    let mut control = RowWork::new(&mut *work);
    let mut cursor = selection.cursor();
    for hash in values {
        let row = cursor
            .next_observed(&mut |n| control.observe_metadata(n))?
            .ok_or(ValueError::Invariant(ValueViolation::Bounds))?;
        control.row()?;
        out[row] = Some(hash);
    }
    control.flush()?;
    Ok(out)
}

/// Positional batch equality. The caller keeps selection to distinguish
/// unselected entries from selected SQL UNKNOWN results.
pub fn equal_rows(
    left: &DecodedBytes,
    right: &DecodedBytes,
    selection: &SelectionSet,
    nulls: NullEquality,
) -> Result<Vec<Option<bool>>, ValueError> {
    equal_rows_observed(
        left,
        right,
        selection,
        nulls,
        &mut |_| Ok::<_, Infallible>(()),
        &mut |_| Ok::<_, Infallible>(()),
    )
    .map_err(ValueFailure::into_error)
}
pub fn equal_rows_observed<E>(
    left: &DecodedBytes,
    right: &DecodedBytes,
    selection: &SelectionSet,
    nulls: NullEquality,
    growth: &mut Growth<'_, E>,
    work: &mut Work<'_, E>,
) -> Result<Vec<Option<bool>>, ValueFailure<E>> {
    use crate::batch::{PreparedBytes, RowWork, compare::compare_into};
    if left.len() != right.len() || selection.len() != left.len() {
        return Err(ValueError::Invariant(ValueViolation::UndefinedRow).into());
    }
    let left_batch = PreparedBytes::new_observed(left, selection, &mut |n| work(n))?;
    let right_batch = PreparedBytes::new_observed(right, selection, &mut |n| work(n))?;
    let mut values = initialized_result(left_batch.len(), None, growth, work)?;
    compare_into(&left_batch, &right_batch, nulls, &mut values, &mut |n| {
        work(n)
    })?;
    let mut out = initialized_result(left.len(), None, growth, work)?;
    let mut control = RowWork::new(&mut *work);
    let mut cursor = selection.cursor();
    for value in values {
        let row = cursor
            .next_observed(&mut |n| control.observe_metadata(n))?
            .ok_or(ValueError::Invariant(ValueViolation::Bounds))?;
        control.row()?;
        out[row] = value;
    }
    control.flush()?;
    Ok(out)
}

fn initialized_result<T: Clone, E>(
    count: usize,
    value: T,
    growth: &mut Growth<'_, E>,
    work: &mut Work<'_, E>,
) -> Result<Vec<T>, ValueFailure<E>> {
    let layout =
        Layout::array::<T>(count).map_err(|_| ValueError::Invariant(ValueViolation::Bounds))?;
    if layout.size() != 0 {
        growth(layout).map_err(ValueFailure::Caller)?;
    }
    let mut values = Vec::new();
    values
        .try_reserve_exact(count)
        .map_err(|source| ValueError::Allocation {
            requested: layout.size(),
            source,
        })?;
    let block = (crate::batch::BYTE_BLOCK / std::mem::size_of::<T>().max(1)).max(1);
    while values.len() < count {
        let end = (values.len() + block).min(count);
        work((end - values.len()) * std::mem::size_of::<T>()).map_err(ValueFailure::Caller)?;
        values.resize(end, value.clone());
    }
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ByteLayout, access::ByteSource, decode::ValueConversionContext, reachability::ReachableRows,
    };
    use arrow_array::{
        ArrayRef, DictionaryArray, Int32Array, LargeStringArray, StringArray, StringViewArray,
        types::Int32Type,
    };
    use std::sync::Arc;
    fn decode(a: &ArrayRef, layout: ByteLayout) -> DecodedBytes {
        let s = ByteSource::parse(a, layout, None).unwrap();
        ValueConversionContext::default()
            .decode(&s, &ReachableRows::all(a.len()))
            .unwrap()
    }
    #[test]
    fn independent_vectors_and_cross_carrier_hashes() {
        let fnv = ByteHashAlgorithm::Fnv1a64 {
            seed: 14695981039346656037,
        };
        let crc = ByteHashAlgorithm::Crc32 { seed: 0 };
        assert_eq!(fnv.hash_bytes(b"hello"), 0xa430d84680aabd0b);
        assert_eq!(crc.hash_bytes(b"123456789"), 0xcbf43926);
        let expected = [
            Some(""),
            None,
            Some("twelve bytes"),
            Some("thirteen byte"),
            Some("a much longer external value"),
        ];
        let arrays: Vec<(ArrayRef, ByteLayout)> = vec![
            (
                Arc::new(StringArray::from(expected.to_vec())),
                ByteLayout::Utf8,
            ),
            (
                Arc::new(LargeStringArray::from(expected.to_vec())),
                ByteLayout::LargeUtf8,
            ),
            (
                Arc::new(StringViewArray::from(expected.to_vec())),
                ByteLayout::Utf8View,
            ),
        ];
        let values: ArrayRef = Arc::new(StringArray::from(expected.to_vec()));
        let dictionary: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![Some(0), None, Some(2), Some(3), Some(4)]),
                values,
            )
            .unwrap(),
        );
        let mut decoded: Vec<_> = arrays.iter().map(|(a, l)| decode(a, *l)).collect();
        decoded.push(decode(&dictionary, ByteLayout::Utf8));
        for algorithm in [fnv, crc] {
            for input in &decoded {
                for (row, value) in expected.iter().enumerate() {
                    let oracle = if let Some(value) = value {
                        algorithm.hash_bytes(value.as_bytes())
                    } else {
                        99
                    };
                    assert_eq!(
                        hash_row(input, row, algorithm, NullHash::Value(99)).unwrap(),
                        oracle
                    );
                    assert_eq!(
                        equal_row(input, row, &decoded[0], row, NullEquality::Equal).unwrap(),
                        Some(true)
                    );
                }
            }
        }
        assert_eq!(
            equal_row(&decoded[0], 1, &decoded[1], 1, NullEquality::Unknown).unwrap(),
            None
        );
    }
    #[test]
    fn null_key_and_null_bottom_share_hash_but_keep_mapping_facts() {
        let values: ArrayRef = Arc::new(StringArray::from(vec![None, Some("x")]));
        let input: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![Some(0), None, Some(1)]),
                values,
            )
            .unwrap(),
        );
        let decoded = decode(&input, ByteLayout::Utf8);
        let algorithm = ByteHashAlgorithm::Fnv1a64 { seed: 17 };
        assert_eq!(
            hash_row(&decoded, 0, algorithm, NullHash::PreserveSeed).unwrap(),
            17
        );
        assert_eq!(
            hash_row(&decoded, 1, algorithm, NullHash::PreserveSeed).unwrap(),
            17
        );
        assert_eq!(decoded.mapping().mapped_row(0).unwrap(), Some(0));
        assert_eq!(decoded.mapping().mapped_row(1).unwrap(), None);
    }
    #[test]
    fn observed_hash_and_equality_bound_work_inside_one_large_value() {
        let text = "a".repeat(150_000);
        let array: ArrayRef = Arc::new(StringArray::from(vec![text.as_str()]));
        let decoded = decode(&array, ByteLayout::Utf8);
        for algorithm in [
            ByteHashAlgorithm::Fnv1a64 {
                seed: 14695981039346656037,
            },
            ByteHashAlgorithm::Crc32 { seed: 19 },
        ] {
            let mut chunks = Vec::new();
            let actual = hash_row_observed(&decoded, 0, algorithm, NullHash::Value(0), &mut |n| {
                chunks.push(n);
                Ok::<_, Infallible>(())
            })
            .unwrap();
            assert_eq!(actual, algorithm.hash_bytes(text.as_bytes()));
            assert_eq!(chunks, vec![65536, 65536, 18928]);
            let mut calls = 0;
            assert!(matches!(
                hash_row_observed(&decoded, 0, algorithm, NullHash::Value(0), &mut |_| {
                    calls += 1;
                    if calls == 2 { Err(71) } else { Ok(()) }
                }),
                Err(ValueFailure::Caller(71))
            ));
        }
        let mut calls = 0;
        assert!(matches!(
            equal_row_observed(&decoded, 0, &decoded, 0, NullEquality::Equal, &mut |_| {
                calls += 1;
                if calls == 2 { Err(72) } else { Ok(()) }
            }),
            Err(ValueFailure::Caller(72))
        ));
        let selection = ReachableRows::all(1)
            .materialize(&mut |_| Ok::<_, Infallible>(()))
            .unwrap();
        assert_eq!(
            equal_rows(&decoded, &decoded, &selection, NullEquality::Equal).unwrap(),
            vec![Some(true)]
        );
    }
    #[test]
    fn compatibility_batches_check_empty_bitmap_work_before_full_scan() {
        let array: ArrayRef = Arc::new(StringViewArray::new_null(200_000));
        let decoded = decode(&array, ByteLayout::Utf8View);
        let selected = SelectionSet::try_new(200_000).unwrap();
        let hash = hash_rows_observed(
            &decoded,
            &selected,
            ByteHashAlgorithm::Fnv1a64 { seed: 17 },
            NullHash::Value(0),
            &mut |_| Ok(()),
            &mut |units| if units == 0 { Ok(()) } else { Err("metadata") },
        );
        assert!(matches!(hash, Err(ValueFailure::Caller("metadata"))));
        let equality = equal_rows_observed(
            &decoded,
            &decoded,
            &selected,
            NullEquality::Equal,
            &mut |_| Ok(()),
            &mut |units| if units == 0 { Ok(()) } else { Err("metadata") },
        );
        assert!(matches!(equality, Err(ValueFailure::Caller("metadata"))));
    }
}
