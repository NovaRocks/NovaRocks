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

use arrow::array::{ArrayRef, BinaryBuilder};

use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};

use super::common::{binary_value_or_empty, row_count, row_index};

pub(crate) fn approx_count_distinct_state_union(a: &[u8], b: &[u8]) -> Result<Vec<u8>, String> {
    super::min_max::min_state_union(a, b)
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, BinaryArray, BinaryBuilder};
    use arrow::datatypes::DataType;

    use super::*;
    use crate::connector::starrocks::managed::state_codec::{MultisetEntry, encode_multiset};

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
}
