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

use std::cell::RefCell;
use std::sync::Arc;

use arrow::array::{ArrayRef, BinaryBuilder, Int64Builder};
use arrow::datatypes::DataType;

use crate::connector::starrocks::managed::state_codec::{
    KeyValue, decode_multiset_self_describing, read_key,
};
use crate::exec::chunk::Chunk;
use crate::exec::expr::agg::{
    HLL_REGISTERS_COUNT, estimate_cardinality_from_registers, hash_bytes_for_hll,
    update_register_from_hash,
};
use crate::exec::expr::{ExprArena, ExprId};

use super::common::{binary_value_or_empty, row_count, row_index};

thread_local! {
    static HLL_REGISTERS: RefCell<[u8; HLL_REGISTERS_COUNT]> =
        RefCell::new([0u8; HLL_REGISTERS_COUNT]);
}

pub(crate) fn approx_count_distinct_state_union(a: &[u8], b: &[u8]) -> Result<Vec<u8>, String> {
    super::min_max::min_state_union(a, b)
}

pub(crate) fn approx_count_distinct_state_visible(s: &[u8]) -> Result<i64, String> {
    let (key_type, entries) = decode_multiset_self_describing(s)?;
    if entries.is_empty() {
        return Ok(0);
    }

    HLL_REGISTERS.with(|cell| {
        let mut registers = cell.borrow_mut();
        registers.fill(0);
        for entry in entries.iter().filter(|entry| entry.count > 0) {
            let hash = hash_multiset_key_for_hll(&key_type, &entry.key_bytes)?;
            update_register_from_hash(&mut registers, hash);
        }
        Ok(estimate_cardinality_from_registers(&registers))
    })
}

fn hash_multiset_key_for_hll(key_type: &DataType, key_bytes: &[u8]) -> Result<u64, String> {
    let mut cursor = key_bytes;
    let key = read_key(&mut cursor, key_type)?;
    if !cursor.is_empty() {
        return Err("approx_count_distinct_state_visible key has trailing bytes".to_string());
    }
    match key {
        KeyValue::Utf8(value) => Ok(hash_bytes_for_hll(value.as_bytes())),
        _ => Ok(hash_bytes_for_hll(key_bytes)),
    }
}

pub(crate) fn eval_approx_count_distinct_state_union(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if args.len() != 2 {
        return Err(format!(
            "approx_count_distinct_state_union expects 2 arguments, got {}",
            args.len()
        ));
    }
    let lhs = arena.eval(args[0], chunk)?;
    let rhs = arena.eval(args[1], chunk)?;
    eval_approx_count_distinct_state_union_arrays(&lhs, &rhs)
}

pub(crate) fn eval_approx_count_distinct_state_visible(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if args.len() != 1 {
        return Err(format!(
            "approx_count_distinct_state_visible expects 1 argument, got {}",
            args.len()
        ));
    }
    let input = arena.eval(args[0], chunk)?;
    eval_approx_count_distinct_state_visible_array(&input)
}

fn eval_approx_count_distinct_state_union_arrays(
    lhs: &ArrayRef,
    rhs: &ArrayRef,
) -> Result<ArrayRef, String> {
    let rows = row_count("approx_count_distinct_state_union", lhs.len(), rhs.len())?;
    let mut builder = BinaryBuilder::new();
    for row in 0..rows {
        let left = binary_value_or_empty(
            lhs,
            row_index(row, lhs.len())?,
            "approx_count_distinct_state_union",
            0,
        )?;
        let right = binary_value_or_empty(
            rhs,
            row_index(row, rhs.len())?,
            "approx_count_distinct_state_union",
            1,
        )?;
        builder.append_value(approx_count_distinct_state_union(left, right)?);
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

fn eval_approx_count_distinct_state_visible_array(input: &ArrayRef) -> Result<ArrayRef, String> {
    let mut builder = Int64Builder::new();
    for row in 0..input.len() {
        let state = binary_value_or_empty(input, row, "approx_count_distinct_state_visible", 0)?;
        builder.append_value(approx_count_distinct_state_visible(state)?);
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        Array, ArrayRef, BinaryArray, BinaryBuilder, Float64Array, Int64Array, StringArray,
    };
    use arrow::datatypes::DataType;

    use super::*;
    use crate::connector::starrocks::managed::state_codec::{
        MultisetEntry, encode_multiset, write_key_at,
    };
    use crate::exec::expr::agg::{
        HLL_REGISTERS_COUNT, estimate_cardinality_from_registers, hash_array_value_for_hll,
        update_register_from_hash,
    };

    fn binary_array(values: &[Option<Vec<u8>>]) -> ArrayRef {
        let mut builder = BinaryBuilder::new();
        for value in values {
            match value {
                Some(bytes) => builder.append_value(bytes),
                None => builder.append_null(),
            }
        }
        Arc::new(builder.finish())
    }

    fn int64_state(entries: &[(i64, i64)]) -> Vec<u8> {
        let entries = entries
            .iter()
            .map(|(key, count)| MultisetEntry {
                key_bytes: key.to_le_bytes().to_vec(),
                count: *count,
            })
            .collect::<Vec<_>>();
        encode_multiset(&entries, &DataType::Int64).unwrap()
    }

    fn state_from_array(array: &ArrayRef, counts: &[i64]) -> Vec<u8> {
        assert_eq!(array.len(), counts.len());
        let mut entries = Vec::new();
        for (row, count) in counts.iter().enumerate() {
            if array.is_null(row) {
                continue;
            }
            let mut key_bytes = Vec::new();
            write_key_at(&mut key_bytes, array, row).unwrap();
            entries.push(MultisetEntry {
                key_bytes,
                count: *count,
            });
        }
        encode_multiset(&entries, array.data_type()).unwrap()
    }

    fn plain_hll_estimate(array: &ArrayRef) -> i64 {
        let mut registers = [0u8; HLL_REGISTERS_COUNT];
        for row in 0..array.len() {
            if let Some(hash) = hash_array_value_for_hll(array, row).unwrap() {
                update_register_from_hash(&mut registers, hash);
            }
        }
        estimate_cardinality_from_registers(&registers)
    }

    #[test]
    fn approx_count_distinct_state_union_shares_count_distinct_union() {
        let left = int64_state(&[(1, 2)]);
        let right = int64_state(&[(2, 3)]);

        let approx = approx_count_distinct_state_union(&left, &right).unwrap();
        let exact =
            super::super::count_distinct::count_distinct_state_union(&left, &right).unwrap();

        assert_eq!(approx, exact);
    }

    #[test]
    fn approx_count_distinct_state_union_arrays_treat_null_as_empty() {
        let lhs = binary_array(&[Some(int64_state(&[(1, 1)])), None]);
        let rhs = binary_array(&[Some(int64_state(&[(2, 1)])), Some(int64_state(&[(3, 1)]))]);

        let out = eval_approx_count_distinct_state_union_arrays(&lhs, &rhs).unwrap();
        let arr = out.as_any().downcast_ref::<BinaryArray>().unwrap();

        let exact0 = super::super::count_distinct::count_distinct_state_union(
            &int64_state(&[(1, 1)]),
            &int64_state(&[(2, 1)]),
        )
        .unwrap();
        let exact1 =
            super::super::count_distinct::count_distinct_state_union(&[], &int64_state(&[(3, 1)]))
                .unwrap();

        assert_eq!(arr.value(0), exact0.as_slice());
        assert_eq!(arr.value(1), exact1.as_slice());
    }

    #[test]
    fn approx_count_distinct_visible_matches_plain_hll_int64() {
        let values = (0..100).map(Some).collect::<Vec<Option<i64>>>();
        let input = Arc::new(Int64Array::from(values)) as ArrayRef;
        let state = state_from_array(&input, &vec![1; input.len()]);

        assert_eq!(
            approx_count_distinct_state_visible(&state).unwrap(),
            plain_hll_estimate(&input)
        );
    }

    #[test]
    fn approx_count_distinct_visible_matches_plain_hll_utf8() {
        let input = Arc::new(StringArray::from(vec![
            Some("alpha"),
            Some("beta"),
            Some("gamma"),
            Some("alpha"),
            None,
        ])) as ArrayRef;
        let state = state_from_array(&input, &[2, 1, 1, 2, 1]);

        assert_eq!(
            approx_count_distinct_state_visible(&state).unwrap(),
            plain_hll_estimate(&input)
        );
    }

    #[test]
    fn approx_count_distinct_visible_canonicalizes_float_like_plain_hll() {
        let input = Arc::new(Float64Array::from(vec![
            Some(0.0),
            Some(-0.0),
            Some(f64::NAN),
            Some(f64::from_bits(0x7ff0_0000_0000_0001)),
        ])) as ArrayRef;
        let state = state_from_array(&input, &vec![1; input.len()]);

        assert_eq!(
            approx_count_distinct_state_visible(&state).unwrap(),
            plain_hll_estimate(&input)
        );
    }

    #[test]
    fn approx_count_distinct_visible_ignores_multiplicity() {
        let one = int64_state(&[(7, 1)]);
        let many = int64_state(&[(7, 100)]);

        assert_eq!(
            approx_count_distinct_state_visible(&one).unwrap(),
            approx_count_distinct_state_visible(&many).unwrap()
        );
    }

    #[test]
    fn approx_count_distinct_visible_empty_returns_zero() {
        assert_eq!(approx_count_distinct_state_visible(&[]).unwrap(), 0);
    }

    #[test]
    fn approx_count_distinct_visible_skips_non_positive() {
        let state = int64_state(&[(1, 1), (2, 0), (3, -1)]);
        let expected = plain_hll_estimate(&(Arc::new(Int64Array::from(vec![Some(1)])) as ArrayRef));

        assert_eq!(
            approx_count_distinct_state_visible(&state).unwrap(),
            expected
        );
    }

    #[test]
    fn approx_count_distinct_visible_arrays_treat_null_as_empty() {
        let input = binary_array(&[Some(int64_state(&[(1, 1), (2, 1)])), None]);

        let out = eval_approx_count_distinct_state_visible_array(&input).unwrap();
        let arr = out
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();

        assert_eq!(
            arr.value(0),
            approx_count_distinct_state_visible(
                input
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .unwrap()
                    .value(0)
            )
            .unwrap()
        );
        assert_eq!(arr.value(1), 0);
    }
}
