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

use std::sync::Arc;

use arrow::array::{ArrayRef, BinaryBuilder, Int64Builder};

use crate::connector::starrocks::managed::state_codec::{
    MultisetEntry, decode_multiset_self_describing,
};
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};

use super::common::{binary_value_or_empty, row_count, row_index};

pub(crate) fn count_distinct_state_union(a: &[u8], b: &[u8]) -> Result<Vec<u8>, String> {
    super::min_max::min_state_union(a, b)
}

pub(crate) fn count_distinct_state_visible(s: &[u8]) -> Result<i64, String> {
    let (_, entries) = decode_multiset_self_describing(s)?;
    positive_entry_count(&entries)
}

pub(crate) fn eval_count_distinct_state_union(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if args.len() != 2 {
        return Err(format!(
            "count_distinct_state_union expects 2 arguments, got {}",
            args.len()
        ));
    }
    let lhs = arena.eval(args[0], chunk)?;
    let rhs = arena.eval(args[1], chunk)?;
    eval_count_distinct_state_union_arrays(&lhs, &rhs)
}

pub(crate) fn eval_count_distinct_state_visible(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if args.len() != 1 {
        return Err(format!(
            "count_distinct_state_visible expects 1 argument, got {}",
            args.len()
        ));
    }
    let input = arena.eval(args[0], chunk)?;
    eval_count_distinct_state_visible_array(&input)
}

fn eval_count_distinct_state_union_arrays(
    lhs: &ArrayRef,
    rhs: &ArrayRef,
) -> Result<ArrayRef, String> {
    let rows = row_count("count_distinct_state_union", lhs.len(), rhs.len())?;
    let mut builder = BinaryBuilder::new();
    for row in 0..rows {
        let left = binary_value_or_empty(
            lhs,
            row_index(row, lhs.len())?,
            "count_distinct_state_union",
            0,
        )?;
        let right = binary_value_or_empty(
            rhs,
            row_index(row, rhs.len())?,
            "count_distinct_state_union",
            1,
        )?;
        builder.append_value(count_distinct_state_union(left, right)?);
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

fn eval_count_distinct_state_visible_array(input: &ArrayRef) -> Result<ArrayRef, String> {
    let mut builder = Int64Builder::new();
    for row in 0..input.len() {
        let state = binary_value_or_empty(input, row, "count_distinct_state_visible", 0)?;
        builder.append_value(count_distinct_state_visible(state)?);
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

fn positive_entry_count(entries: &[MultisetEntry]) -> Result<i64, String> {
    entries
        .iter()
        .filter(|entry| entry.count > 0)
        .count()
        .try_into()
        .map_err(|_| "count_distinct_state_visible count overflow".to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, BinaryArray, BinaryBuilder, Int64Array};
    use arrow::datatypes::DataType;

    use super::*;
    use crate::connector::starrocks::managed::state_codec::encode_multiset;

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

    #[test]
    fn count_distinct_state_visible_counts_positive_entries() {
        let state = int64_state(&[(1, 5), (2, 1), (3, 2)]);

        assert_eq!(count_distinct_state_visible(&state).unwrap(), 3);
    }

    #[test]
    fn count_distinct_state_visible_skips_zero_or_negative() {
        let state = int64_state(&[(1, 5), (2, 0), (3, -1)]);

        assert_eq!(count_distinct_state_visible(&state).unwrap(), 1);
    }

    #[test]
    fn count_distinct_state_visible_empty_returns_zero() {
        assert_eq!(count_distinct_state_visible(&[]).unwrap(), 0);
    }

    #[test]
    fn count_distinct_state_union_shares_multiset_union() {
        let left = int64_state(&[(1, 2)]);
        let right = int64_state(&[(2, 3)]);

        let count_distinct = count_distinct_state_union(&left, &right).unwrap();
        let min = super::super::min_max::min_state_union(&left, &right).unwrap();

        assert_eq!(count_distinct, min);
    }

    #[test]
    fn count_distinct_state_visible_arrays_treat_null_as_empty() {
        let input = binary_array(&[Some(int64_state(&[(1, 2), (2, -1)])), None]);

        let out = eval_count_distinct_state_visible_array(&input).unwrap();
        let arr = out.as_any().downcast_ref::<Int64Array>().unwrap();

        assert_eq!(arr.value(0), 1);
        assert_eq!(arr.value(1), 0);
    }

    #[test]
    fn count_distinct_state_union_arrays_treat_null_as_empty() {
        let lhs = binary_array(&[Some(int64_state(&[(1, 1)])), None]);
        let rhs = binary_array(&[Some(int64_state(&[(2, 1)])), Some(int64_state(&[(3, 1)]))]);

        let out = eval_count_distinct_state_union_arrays(&lhs, &rhs).unwrap();
        let arr = out.as_any().downcast_ref::<BinaryArray>().unwrap();

        assert_eq!(count_distinct_state_visible(arr.value(0)).unwrap(), 2);
        assert_eq!(count_distinct_state_visible(arr.value(1)).unwrap(), 1);
    }
}
